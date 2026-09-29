import asyncio
import sys
from pathlib import Path

import pytest
import pytest_asyncio

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import app  # noqa: E402
from app import db as db_module  # noqa: E402


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "test_firmware.db"


@pytest_asyncio.fixture()
async def client(app_db):
    db_path = app_db

    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture()
async def app_db(db_path):
    """Point the shared app at a fresh temp DB (used by in-process and TCP tests)."""
    app.state.db_path = db_path
    await db_module.init_db(db_path)
    return db_path


def run(coro):
    """Run a coroutine to completion in a fresh loop (used inside worker threads)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
