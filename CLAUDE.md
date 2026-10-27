# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Newly bootstrapped — no source code yet (as of 2026-10-27). This file must be updated once the project takes shape: build/test commands, architecture, port registration.

## Monorepo Context

This is one of the `fusion-*` sub-projects in `/Users/dahai/fusion` (local-first Apple Silicon AI ecosystem). Key rules inherited from the workspace:

- **Own git repo**: each `fusion-*` project is an independent git repository. Commit/branch/PR here, never at the workspace root. This project has no `.git` yet — `git init` before first commit.
- **Shared venv**: `cd /Users/dahai/fusion && source .venv/bin/activate` before any Python work. Python 3.12 (workspace-unified `.python-version`).
- **Port registry**: if this project runs a service, register its port in `architecture/port-registry.yaml` and validate with `python scripts/check-ports.py` from the workspace root. Never reuse a registered port.
- **LLM access**: local inference via `fusion-mlx` (`localhost:11434`, OpenAI-compatible API); route downstream traffic through `fusion-gateway` (`:11432`). Model downloads via `https://hf-mirror.com`.
- **Common deps**: Python projects use `pydantic` + `httpx`, `fastapi` + `uvicorn` for APIs, `pytest` with `asyncio_mode=auto`, `ruff` for lint (configure `[tool.ruff]` in pyproject.toml).
- **fusion-core**: in-tree shared library (`pip install -e fusion-core`) — LLM client, JSON parsing, config, logging, FastAPI factory. Prefer it over bespoke implementations.
- **Code style**: 4-space-multiple indentation, no docstrings, always include logging.

## Workspace References

- `/Users/dahai/fusion/CLAUDE.md` — full monorepo guide (project layers, build commands, conventions)
- `/Users/dahai/fusion/architecture/` — design docs, PRDs, project relationships, port registry
- `/Users/dahai/fusion/architecture/PROJECT_RELATIONSHIPS.md` — inter-project dependencies
