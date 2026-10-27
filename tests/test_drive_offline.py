import anyio
import pytest

from fusion_flow import Job, JobSpec
from fusion_flow.connectors import DriveConnector
from fusion_flow.errors import ConnectorError


class FakeDriveClient:
    # In-memory Drive transport with a hard offline switch. The token is the
    # number of changes consumed: changes_list returns everything after the
    # given index, so an un-advanced token naturally re-delivers files.
    def __init__(self):
        self.online = True
        self.changes = []
        self.downloads = []
        self.uploads = []
        self.moves = []

    def add_file(self, file_id, name, folder_id):
        self.changes.append(
            {"file_id": file_id, "file": {"id": file_id, "name": name, "folder_id": folder_id}}
        )

    async def start_page_token(self):
        if not self.online:
            raise ConnectionError("offline")
        return f"tok-{len(self.changes)}"

    async def changes_list(self, page_token):
        if not self.online:
            raise ConnectionError("offline")
        idx = int(page_token.split("-")[1])
        return f"tok-{len(self.changes)}", [dict(c) for c in self.changes[idx:]]

    async def download(self, file_id, dest):
        if not self.online:
            raise ConnectionError("offline")
        self.downloads.append((file_id, str(dest)))
        dest.write_text(f"content-{file_id}")
        return dest

    async def upload(self, local_path, folder_id, name=None):
        if not self.online:
            raise ConnectionError("offline")
        file_id = f"up-{len(self.uploads)}"
        self.uploads.append((str(local_path), folder_id, name))
        return file_id

    async def move(self, file_id, folder_id):
        if not self.online:
            raise ConnectionError("offline")
        self.moves.append((file_id, folder_id))


async def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.01):
    deadline = anyio.current_time() + timeout
    while anyio.current_time() < deadline:
        if predicate():
            return
        await anyio.sleep(interval)
    raise AssertionError("condition not met within timeout")


def make_connector(flow, client, **kwargs):
    def on_new_file(folder_id, file):
        return JobSpec(
            kind="photo",
            payload={"file_id": file["id"], "name": file["name"]},
            dedup_key=f"drive:{file['id']}",
        )

    return DriveConnector(client, on_new_file=on_new_file, event_bus=flow.event_bus, **kwargs)


async def test_baseline_token_skips_existing_files(flow):
    client = FakeDriveClient()
    client.add_file("pre1", "old.png", "folder-a")
    conn = make_connector(flow, client)

    # First poll: establish baseline at CURRENT state, no backfill.
    specs = await conn.poll()
    assert specs == []
    await conn.commit()
    assert flow.event_bus.kv_get("drive:page_token") == "tok-1"

    client.add_file("new1", "new.png", "folder-a")
    specs = await conn.poll()
    assert [s.payload["file_id"] for s in specs] == ["new1"]
    await conn.commit()
    assert flow.event_bus.kv_get("drive:page_token") == "tok-2"


async def test_uncommitted_poll_repulls_everything(flow):
    # Crash before commit: kv token untouched, so a fresh connector instance
    # (process restart) re-pulls the same files — zero loss by construction.
    client = FakeDriveClient()
    conn = make_connector(flow, client)
    assert await conn.poll() == []
    await conn.commit()
    assert flow.event_bus.kv_get("drive:page_token") == "tok-0"

    client.add_file("f1", "a.png", "folder-a")
    specs = await conn.poll()
    assert [s.payload["file_id"] for s in specs] == ["f1"]
    # No commit — simulate hard crash, rebuild connector from durable state.
    conn2 = make_connector(flow, client)
    specs2 = await conn2.poll()
    assert [s.payload["file_id"] for s in specs2] == ["f1"]
    await conn2.commit()
    assert flow.event_bus.kv_get("drive:page_token") == "tok-1"


async def test_b1_b2_outage_zero_loss_end_to_end(flow):
    # B1: network dies mid-polling — backoff retries, no crash, token frozen.
    # B2: network returns — every file created during the outage is pulled,
    # zero files lost, no duplicates (dedup_key = drive file id).
    client = FakeDriveClient()
    conn = make_connector(flow, client)
    flow.attach_connector(conn, name="drive", interval_s=0.02, backoff_base_s=0.01)

    @flow.handler("photo")
    async def photo(ctx, payload):
        return f"ref://{payload['file_id']}"

    async with flow.run():
        runner = flow.connectors[0]
        # Baseline established.
        await _wait_until(lambda: flow.event_bus.kv_get("drive:page_token") is not None)

        client.add_file("f1", "a.png", "folder-a")
        client.add_file("f2", "b.png", "folder-a")
        await _wait_until(lambda: flow.stats().by_status.get("succeeded", 0) >= 2)
        await _wait_until(lambda: runner.commit_count >= 2)

        # B1: outage — new files arrive but poll fails repeatedly.
        client.online = False
        client.add_file("f3", "c.png", "folder-a")
        client.add_file("f4", "d.png", "folder-a")
        token_before = flow.event_bus.kv_get("drive:page_token")
        await _wait_until(lambda: runner.failures >= 2)
        # Token strictly not advanced past the last committed state.
        assert flow.event_bus.kv_get("drive:page_token") == token_before
        reasons = [e.data.get("reason", "") for e in flow.events(limit=300)]
        assert any("offline" in r for r in reasons)

        # B2: recovery — f3/f4 (created during outage) all arrive.
        client.online = True
        await _wait_until(lambda: flow.stats().by_status.get("succeeded", 0) >= 4)
        done = {(j.payload["file_id"]) for j in flow.list_jobs(kind="photo")}
        assert done == {"f1", "f2", "f3", "f4"}
        # No duplicates ever: 4 files, 4 jobs.
        assert flow.stats().by_status.get("succeeded", 0) == 4


async def test_primitives_delegate_to_client(flow, tmp_path):
    client = FakeDriveClient()
    conn = make_connector(
        flow,
        client,
        inbox_dir=tmp_path / "inbox",
        output_folder_id="out-folder",
        rejected_folder_id="rej-folder",
    )
    dest = await conn.download_to_inbox("f1", "a.png")
    assert dest.read_text() == "content-f1"
    assert client.downloads == [("f1", str(tmp_path / "inbox" / "a.png"))]

    out_id = await conn.upload_output(dest, name="job-1")
    assert out_id == "up-0"
    assert client.uploads == [(str(dest), "out-folder", "job-1")]

    await conn.move_to_rejected("f1")
    assert client.moves == [("f1", "rej-folder")]


async def test_primitives_require_config(flow, tmp_path):
    client = FakeDriveClient()
    conn = make_connector(flow, client)

    with pytest.raises(ConnectorError):
        await conn.download_to_inbox("f1")
    with pytest.raises(ConnectorError):
        await conn.upload_output(tmp_path / "x.png")
    with pytest.raises(ConnectorError):
        await conn.move_to_rejected("f1")


async def test_on_job_done_uploads_result(flow, tmp_path):
    client = FakeDriveClient()
    result_file = tmp_path / "result.png"
    result_file.write_text("png")
    conn = make_connector(flow, client, output_folder_id="out-folder")

    job = Job(id="flw_test", kind="photo", payload={})
    await conn.on_job_done(job, str(result_file))
    assert client.uploads == [(str(result_file), "out-folder", "flw_test")]

    # No result_ref -> no upload, no crash.
    await conn.on_job_done(job, None)
    assert len(client.uploads) == 1


async def test_on_new_file_callback_failure_skips_file(flow):
    client = FakeDriveClient()
    conn = make_connector(flow, client)
    assert await conn.poll() == []
    await conn.commit()
    client.add_file("bad", "x.png", "folder-a")
    client.add_file("good", "y.png", "folder-a")

    def on_new_file(folder_id, file):
        if file["id"] == "bad":
            raise ValueError("business mapping bug")
        return JobSpec(
            kind="photo", payload={"file_id": file["id"]}, dedup_key=f"drive:{file['id']}"
        )

    conn = DriveConnector(client, on_new_file=on_new_file, event_bus=flow.event_bus)
    specs = await conn.poll()
    assert [s.payload["file_id"] for s in specs] == ["good"]
    # Round still commits so the broken file is not re-delivered forever.
    await conn.commit()
    assert flow.event_bus.kv_get("drive:page_token") == "tok-2"
