"""Local firmware staged-rollout HTTP API.

See README.md for the full call flow. Key guarantees:

* A group has at most one unfinished batch (enforced by a partial UNIQUE index).
* Every receipt submission is a single ``BEGIN IMMEDIATE`` transaction; the
  "any failed -> batch fails" and "all ready -> atomically switch every
  version" transitions therefore never expose partial state and are
  equivalent to some serial ordering under concurrency.
* After a batch ends, only replays of its existing receipts are accepted;
  brand-new late receipts are rejected and can never affect a later batch.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aiosqlite
from fastapi import Depends, FastAPI, HTTPException, Request, status

from . import db
from .schemas import (
    BatchListResponse,
    BatchResponse,
    BatchSummary,
    CreateBatchRequest,
    CreateBatchResponse,
    DeviceInfo,
    GroupResponse,
    ReceiptInfo,
    RegisterGroupRequest,
    SubmitReceiptRequest,
    SubmitReceiptResponse,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@asynccontextmanager
async def lifespan(app: FastAPI):
    path = getattr(app.state, "db_path", None) or db.DB_PATH
    app.state.db_path = path
    await db.init_db(path)
    yield


app = FastAPI(title="Firmware Staged Rollout API", version="1.0.0", lifespan=lifespan)


async def get_db(request: Request) -> aiosqlite.Connection:
    """Per-request connection with one dedicated transaction boundary."""
    async with db.connect(request.app.state.db_path) as conn:
        yield conn


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# --------------------------------------------------------------------------- #
# Device groups
# --------------------------------------------------------------------------- #
@app.put("/groups/{group_id}", response_model=GroupResponse)
async def register_group(
    group_id: str,
    body: RegisterGroupRequest,
    conn: aiosqlite.Connection = Depends(get_db),
):
    """Register a device group (2-8 unique devices with current versions).

    Re-registering the *same* group (identical device set and versions) is an
    idempotent no-op. Re-registering with changed devices/versions conflicts.
    """
    devices = body.devices
    blanks = [d for d, v in devices.items() if not d.strip() or not v.strip()]
    if blanks:
        raise HTTPException(
            status_code=422,
            detail="device_id and version must be non-empty strings",
        )

    created_at = utc_now()
    try:
        async with db.write_tx(conn):
            async with conn.execute(
                "SELECT created_at FROM groups WHERE group_id = ?", (group_id,)
            ) as cur:
                row = await cur.fetchone()

            if row is None:
                await conn.execute(
                    "INSERT INTO groups(group_id, created_at) VALUES (?, ?)",
                    (group_id, created_at),
                )
                await conn.executemany(
                    "INSERT INTO devices(group_id, device_id, current_version) "
                    "VALUES (?, ?, ?)",
                    [(group_id, d, v) for d, v in devices.items()],
                )
            else:
                async with conn.execute(
                    "SELECT device_id, current_version FROM devices WHERE group_id = ?",
                    (group_id,),
                ) as cur:
                    existing = {r["device_id"]: r["current_version"] for r in await cur.fetchall()}
                if existing != devices:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="group already registered with a different device set/version; "
                        "re-registration with changes is not allowed",
                    )
                created_at = row["created_at"]
    except aiosqlite.OperationalError as exc:  # pragma: no cover - lock exhaustion
        if "locked" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="database busy, please retry",
            ) from exc
        raise

    return await _load_group(conn, group_id)


@app.get("/groups/{group_id}", response_model=GroupResponse)
async def get_group(group_id: str, conn: aiosqlite.Connection = Depends(get_db)):
    """Return the group's devices and their current versions."""
    return await _load_group(conn, group_id)


async def _load_group(conn: aiosqlite.Connection, group_id: str) -> GroupResponse:
    async with db.read_tx(conn):
        async with conn.execute(
            "SELECT created_at FROM groups WHERE group_id = ?", (group_id,)
        ) as cur:
            group_row = await cur.fetchone()
            if group_row is None:
                raise HTTPException(status_code=404, detail="group not found")
            async with conn.execute(
                "SELECT device_id, current_version FROM devices "
                "WHERE group_id = ? ORDER BY device_id",
                (group_id,),
            ) as cur:
                dev_rows = await cur.fetchall()
            async with conn.execute(
                "SELECT batch_id FROM batches WHERE group_id = ? AND status = 'pending'",
                (group_id,),
            ) as cur:
                pending = await cur.fetchone()

    return GroupResponse(
        group_id=group_id,
        created_at=group_row["created_at"],
        devices=[
            DeviceInfo(device_id=r["device_id"], current_version=r["current_version"])
            for r in dev_rows
        ],
        open_batch_id=pending["batch_id"] if pending else None,
    )


# --------------------------------------------------------------------------- #
# Batches
# --------------------------------------------------------------------------- #
@app.post(
    "/groups/{group_id}/batches",
    response_model=CreateBatchResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_batch(
    group_id: str,
    body: CreateBatchRequest,
    conn: aiosqlite.Connection = Depends(get_db),
):
    """Open a new release batch for the whole group.

    Rejected (409) while the group still has an unfinished batch.
    Targets must name exactly the group's devices (422 otherwise).
    """
    targets = body.targets
    if any(not d.strip() or not v.strip() for d, v in targets.items()):
        raise HTTPException(
            status_code=422,
            detail="device_id and target version must be non-empty strings",
        )

    batch_id = uuid.uuid4().hex
    created_at = utc_now()
    try:
        async with db.write_tx(conn):
            async with conn.execute(
                "SELECT 1 FROM groups WHERE group_id = ?", (group_id,)
            ) as cur:
                if await cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="group not found")

            async with conn.execute(
                "SELECT batch_id FROM batches WHERE group_id = ? AND status = 'pending'",
                (group_id,),
            ) as cur:
                open_row = await cur.fetchone()
                if open_row is not None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"group has an unfinished batch {open_row['batch_id']}",
                    )

            async with conn.execute(
                "SELECT device_id FROM devices WHERE group_id = ?", (group_id,)
            ) as cur:
                group_devices = {r["device_id"] for r in await cur.fetchall()}

            if set(targets) != group_devices:
                raise HTTPException(
                    status_code=422,
                    detail="targets must cover exactly every device in the group; "
                    f"group devices: {sorted(group_devices)}, got: {sorted(targets)}",
                )

            await conn.execute(
                "INSERT INTO batches(batch_id, group_id, status, created_at) "
                "VALUES (?, ?, 'pending', ?)",
                (batch_id, group_id, created_at),
            )
            await conn.executemany(
                "INSERT INTO batch_targets(batch_id, group_id, device_id, target_version) "
                "VALUES (?, ?, ?, ?)",
                [(batch_id, group_id, d, v) for d, v in targets.items()],
            )
    except aiosqlite.IntegrityError as exc:
        # Lost the race to create the unique pending batch (partial UNIQUE index).
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="group already has an unfinished batch",
        ) from exc
    except aiosqlite.OperationalError as exc:  # pragma: no cover - lock exhaustion
        if "locked" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="database busy, please retry",
            ) from exc
        raise

    return CreateBatchResponse(
        batch_id=batch_id,
        group_id=group_id,
        status="pending",
        targets=targets,
    )


@app.get("/groups/{group_id}/batches", response_model=BatchListResponse)
async def list_batches(group_id: str, conn: aiosqlite.Connection = Depends(get_db)):
    async with db.read_tx(conn):
        async with conn.execute(
            "SELECT 1 FROM groups WHERE group_id = ?", (group_id,)
        ) as cur:
            if await cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="group not found")
            async with conn.execute(
                "SELECT batch_id, status, created_at, ended_at FROM batches "
                "WHERE group_id = ? ORDER BY created_at, batch_id",
                (group_id,),
            ) as cur:
                rows = await cur.fetchall()

    return BatchListResponse(
        group_id=group_id,
        batches=[
            BatchSummary(
                batch_id=r["batch_id"],
                status=r["status"],
                created_at=r["created_at"],
                ended_at=r["ended_at"],
            )
            for r in rows
        ],
    )


@app.get("/batches/{batch_id}", response_model=BatchResponse)
async def get_batch(batch_id: str, conn: aiosqlite.Connection = Depends(get_db)):
    """Return batch status, per-device targets and receipts so far."""
    async with db.read_tx(conn):
        async with conn.execute(
            "SELECT group_id, status, created_at, ended_at FROM batches WHERE batch_id = ?",
            (batch_id,),
        ) as cur:
            batch = await cur.fetchone()
            if batch is None:
                raise HTTPException(status_code=404, detail="batch not found")
            async with conn.execute(
                "SELECT device_id, target_version FROM batch_targets "
                "WHERE batch_id = ? ORDER BY device_id",
                (batch_id,),
            ) as cur:
                target_rows = await cur.fetchall()
            async with conn.execute(
                "SELECT device_id, result, created_at FROM receipts "
                "WHERE batch_id = ? ORDER BY created_at",
                (batch_id,),
            ) as cur:
                receipt_rows = await cur.fetchall()

    return BatchResponse(
        batch_id=batch_id,
        group_id=batch["group_id"],
        status=batch["status"],
        created_at=batch["created_at"],
        ended_at=batch["ended_at"],
        targets={r["device_id"]: r["target_version"] for r in target_rows},
        receipts=[
            ReceiptInfo(
                device_id=r["device_id"], result=r["result"], created_at=r["created_at"]
            )
            for r in receipt_rows
        ],
    )


# --------------------------------------------------------------------------- #
# Receipts
# --------------------------------------------------------------------------- #
@app.put(
    "/batches/{batch_id}/devices/{device_id}/receipt",
    response_model=SubmitReceiptResponse,
)
async def submit_receipt(
    batch_id: str,
    device_id: str,
    body: SubmitReceiptRequest,
    conn: aiosqlite.Connection = Depends(get_db),
):
    """Submit a device's ``ready``/``failed`` receipt for a batch.

    * Same device resubmitting the same result is an idempotent success (replay).
    * Same device submitting the other result -> 409 conflict.
    * Any ``failed`` ends the batch as ``failed``; device versions stay untouched.
    * The final ``ready`` switches *all* device versions to targets and marks the
      batch ``succeeded`` in the same transaction.
    * After the batch ends, new (previously unseen) receipts are rejected; only
      replays of recorded receipts remain accepted.
    """
    result = body.result
    now = utc_now()
    replayed = False
    final_status: str | None = None
    ended_at: str | None = None

    try:
        async with db.write_tx(conn):
            async with conn.execute(
                "SELECT group_id, status FROM batches WHERE batch_id = ?", (batch_id,)
            ) as cur:
                batch = await cur.fetchone()
                if batch is None:
                    raise HTTPException(status_code=404, detail="batch not found")
                group_id = batch["group_id"]
                batch_status = batch["status"]

            async with conn.execute(
                "SELECT 1 FROM batch_targets WHERE batch_id = ? AND device_id = ?",
                (batch_id, device_id),
            ) as cur:
                if await cur.fetchone() is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="device is not part of this batch",
                    )

            # Replay of an existing receipt: same result -> idempotent success,
            # changed result -> conflict. Valid in every batch state.
            async with conn.execute(
                "SELECT result FROM receipts WHERE batch_id = ? AND device_id = ?",
                (batch_id, device_id),
            ) as cur:
                existing = await cur.fetchone()
                if existing is not None:
                    if existing["result"] != result:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail=f"device already reported '{existing['result']}'; "
                            f"cannot change result to '{result}'",
                        )
                    replayed = True
                    final_status = batch_status
                    ended_at = (
                        await _ended_at(conn, batch_id)
                        if batch_status != "pending"
                        else None
                    )
                elif batch_status != "pending":
                    # Batch already ended without a receipt from this device:
                    # a late new receipt must not influence this or later batches.
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"batch already {batch_status}; late new receipts are not accepted",
                    )
                else:
                    await conn.execute(
                        "INSERT INTO receipts(batch_id, device_id, result, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (batch_id, device_id, result, now),
                    )

                    if result == "failed":
                        # Any failure fails the batch; versions are never modified.
                        await conn.execute(
                            "UPDATE batches SET status = 'failed', ended_at = ? "
                            "WHERE batch_id = ?",
                            (now, batch_id),
                        )
                        final_status = "failed"
                        ended_at = now
                    else:
                        final_status, ended_at = await _maybe_succeed(
                            conn, batch_id, group_id, now
                        )
    except aiosqlite.OperationalError as exc:  # pragma: no cover - lock exhaustion
        if "locked" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="database busy, please retry",
            ) from exc
        raise

    return SubmitReceiptResponse(
        batch_id=batch_id,
        device_id=device_id,
        result=result,
        status=final_status,
        replayed=replayed,
        ended_at=ended_at,
    )


async def _maybe_succeed(
    conn: aiosqlite.Connection, batch_id: str, group_id: str, now: str
) -> tuple[str, str | None]:
    """If every device has now reported ``ready``, switch all versions atomically.

    Called inside the write transaction, so the version updates and the batch
    status flip commit together — readers can never observe a partial switch.
    """
    async with conn.execute(
        "SELECT COUNT(*) AS n FROM batch_targets WHERE batch_id = ?", (batch_id,)
    ) as cur:
        total = (await cur.fetchone())["n"]
    async with conn.execute(
        "SELECT COUNT(*) AS n FROM receipts WHERE batch_id = ? AND result = 'ready'",
        (batch_id,),
    ) as cur:
        ready = (await cur.fetchone())["n"]

    if ready != total:
        return "pending", None

    # Correlated UPDATE: devices.current_version := this batch's target_version,
    # for every device of the group, in one statement inside this transaction.
    await conn.execute(
        """
        UPDATE devices
           SET current_version = (
                SELECT t.target_version
                  FROM batch_targets t
                 WHERE t.batch_id = ? AND t.device_id = devices.device_id
            )
         WHERE group_id = ?
        """,
        (batch_id, group_id),
    )
    await conn.execute(
        "UPDATE batches SET status = 'succeeded', ended_at = ? WHERE batch_id = ?",
        (now, batch_id),
    )
    return "succeeded", now


async def _ended_at(conn: aiosqlite.Connection, batch_id: str) -> str | None:
    async with conn.execute(
        "SELECT ended_at FROM batches WHERE batch_id = ?", (batch_id,)
    ) as cur:
        row = await cur.fetchone()
        return row["ended_at"] if row else None
