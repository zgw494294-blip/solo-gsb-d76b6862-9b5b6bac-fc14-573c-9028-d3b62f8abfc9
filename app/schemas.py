"""Pydantic request/response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Result = Literal["ready", "failed"]
BatchStatus = Literal["pending", "succeeded", "failed"]


class RegisterGroupRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: device_id -> current firmware version; 2..8 unique devices
    devices: dict[str, str] = Field(min_length=2, max_length=8)


class DeviceInfo(BaseModel):
    device_id: str
    current_version: str


class GroupResponse(BaseModel):
    group_id: str
    created_at: str
    devices: list[DeviceInfo]
    open_batch_id: str | None = None


class CreateBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: device_id -> target firmware version; must cover every group device exactly
    targets: dict[str, str] = Field(min_length=1, max_length=8)


class BatchSummary(BaseModel):
    batch_id: str
    status: BatchStatus
    created_at: str
    ended_at: str | None = None


class CreateBatchResponse(BaseModel):
    batch_id: str
    group_id: str
    status: BatchStatus
    targets: dict[str, str]


class ReceiptInfo(BaseModel):
    device_id: str
    result: Result
    created_at: str


class BatchResponse(BaseModel):
    batch_id: str
    group_id: str
    status: BatchStatus
    created_at: str
    ended_at: str | None = None
    targets: dict[str, str]
    receipts: list[ReceiptInfo]


class BatchListResponse(BaseModel):
    group_id: str
    batches: list[BatchSummary]


class SubmitReceiptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    result: Result


class SubmitReceiptResponse(BaseModel):
    batch_id: str
    device_id: str
    result: Result
    status: BatchStatus
    replayed: bool
    ended_at: str | None = None
