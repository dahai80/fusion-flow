import logging
from pathlib import Path
from typing import Protocol

from ..errors import ConnectorError
from ..event_bus import EventBus
from ..models import Job, JobSpec

logger = logging.getLogger("fusion_flow")


class DriveClient(Protocol):
    # Transport seam for the Drive connector: callers inject a client built
    # on google-api-python-client + fusion-identity credentials (optional
    # `drive` extra). The library itself has ZERO google imports and zero
    # credential logic — tests inject a fake with an offline switch.
    async def start_page_token(self) -> str: ...
    async def changes_list(self, page_token: str) -> tuple[str, list[dict]]: ...
    async def download(self, file_id: str, dest: Path) -> Path: ...
    async def upload(self, local_path: Path, folder_id: str, name: str | None = None) -> str: ...
    async def move(self, file_id: str, folder_id: str) -> None: ...


class DriveConnector:
    # Incremental Drive polling via changes.list. The pageToken lives in
    # flow_kv and is ONLY persisted by commit() — the ConnectorRunner calls
    # commit() after a fully successful round. Network outage mid-round
    # therefore leaves the token untouched and the next poll re-pulls every
    # file seen since (B1/B2 zero-loss).
    #
    # flow_kv contract: the token key NEVER gets a TTL. expires_at is the
    # writer's own declaration; durable connector cursors must not expire.
    #
    # Folder -> business mapping stays OUT of the library: poll() only
    # forwards (folder_id, file) to the injected on_new_file callback, which
    # returns the JobSpec (or None to skip). The library does not know what a
    # "template group" is.

    def __init__(
        self,
        client: DriveClient,
        *,
        on_new_file,
        event_bus: EventBus,
        key_prefix: str = "drive",
        inbox_dir: Path | str | None = None,
        output_folder_id: str | None = None,
        rejected_folder_id: str | None = None,
    ):
        self.client = client
        self.on_new_file = on_new_file
        self.kv = event_bus
        self.token_key = f"{key_prefix}:page_token"
        self.inbox_dir = Path(inbox_dir) if inbox_dir is not None else None
        self.output_folder_id = output_folder_id
        self.rejected_folder_id = rejected_folder_id
        self._pending_token = None

    async def poll(self) -> list[JobSpec]:
        self._pending_token = None
        token = self.kv.kv_get(self.token_key)
        if token is None:
            # First run: establish a baseline token at the CURRENT state so
            # pre-existing files are not enqueued as a giant backfill.
            self._pending_token = await self.client.start_page_token()
            logger.info("drive baseline page token established")
            return []
        new_token, changes = await self.client.changes_list(token)
        specs = []
        for change in changes:
            file = dict(change.get("file") or {})
            folder_id = change.get("folder_id") or file.get("folder_id") or ""
            try:
                spec = self.on_new_file(folder_id, file)
            except Exception:
                # Business mapping bug: skip this file but keep the round
                # alive; the runner still commits, so the file is not
                # re-delivered forever. Log loudly for the audit.
                logger.exception(
                    "drive on_new_file callback failed for %s, file skipped", file.get("id")
                )
                continue
            if spec is not None:
                specs.append(spec)
        self._pending_token = new_token
        logger.info("drive poll: %d changes, %d specs", len(changes), len(specs))
        return specs

    async def commit(self) -> None:
        if self._pending_token is None:
            return
        self.kv.kv_set(self.token_key, self._pending_token)
        self._pending_token = None

    async def on_job_done(self, job: Job, result_ref: str | None) -> None:
        # Reverse path: publish the result back to Drive. A failed reverse
        # path never rolls back the job (already terminal locally); the
        # runner retries, then emits connector_error for compensation.
        if result_ref and self.output_folder_id:
            await self.upload_output(Path(result_ref), name=job.id)
            logger.info("drive uploaded output for job %s", job.id)

    async def download_to_inbox(self, file_id: str, name: str | None = None) -> Path:
        if self.inbox_dir is None:
            raise ConnectorError("inbox_dir not configured")
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        dest = self.inbox_dir / (name or file_id)
        return await self.client.download(file_id, dest)

    async def upload_output(self, local_path: Path, name: str | None = None) -> str:
        if not self.output_folder_id:
            raise ConnectorError("output_folder_id not configured")
        return await self.client.upload(local_path, self.output_folder_id, name)

    async def move_to_rejected(self, file_id: str) -> None:
        if not self.rejected_folder_id:
            raise ConnectorError("rejected_folder_id not configured")
        await self.client.move(file_id, self.rejected_folder_id)
        logger.info("drive moved %s to rejected", file_id)
