"""The public v1 contract. No framework imports belong here."""

from collections.abc import AsyncIterator
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

PROTOCOL_VERSION = 1


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class EventType(StrEnum):
    STARTED = "turn.started"
    DELTA = "assistant.text.delta"
    MESSAGE = "assistant.message.completed"
    TOOL_STARTED = "tool.started"
    TOOL_COMPLETED = "tool.completed"
    TOOL_FAILED = "tool.failed"
    COMPLETED = "turn.completed"
    CANCELLED = "turn.cancelled"
    FAILED = "turn.failed"
    SWITCHED = "runtime.switched"


TERMINAL = {EventType.COMPLETED, EventType.CANCELLED, EventType.FAILED}


class Capabilities(StrictModel):
    incremental_text: bool = True
    shared_tools: bool = True
    cancellation: str = "cooperative"
    state_strategy: str = "bounded_common_history"


class SessionContext(StrictModel):
    version: int = Field(default=PROTOCOL_VERSION, ge=PROTOCOL_VERSION, le=PROTOCOL_VERSION)
    session_id: str = Field(min_length=1, max_length=128)
    workspace: str = Field(min_length=1, max_length=128)
    bot_id: str = Field(min_length=1, max_length=128)
    profile: str = Field(min_length=1, max_length=64)
    trace_id: str = Field(default_factory=lambda: uuid4().hex)


class Turn(StrictModel):
    turn_id: str = Field(default_factory=lambda: uuid4().hex, min_length=1, max_length=128)
    event_id: str = Field(min_length=1, max_length=256)
    text: str = Field(min_length=1, max_length=16000)
    speaker: str = Field(default="Participant", max_length=256)
    speaker_id: str | None = Field(default=None, max_length=128)
    timestamp: float = Field(ge=0)


class RuntimeEvent(StrictModel):
    """Adapters emit output, while the host owns ordering and terminal events."""

    type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)


class AgentEvent(StrictModel):
    version: int = Field(default=PROTOCOL_VERSION, ge=PROTOCOL_VERSION, le=PROTOCOL_VERSION)
    session_id: str
    turn_id: str
    generation: int
    event_id: str = Field(default_factory=lambda: uuid4().hex)
    sequence: int
    type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)


class CancelOutcome(StrictModel):
    output_fenced: bool = True
    runtime_stopped: bool
    state_safe: bool


class AdapterError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code, self.message, self.retryable = code, message, retryable

    def public(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "retryable": self.retryable}


class RuntimeSession(Protocol):
    capabilities: Capabilities

    def run_turn(self, turn: Turn, history: list[dict[str, str]]) -> AsyncIterator[RuntimeEvent]: ...
    async def close(self) -> None: ...


class AgentAdapter(Protocol):
    async def open_session(self, context: SessionContext, tools: Any) -> RuntimeSession: ...
