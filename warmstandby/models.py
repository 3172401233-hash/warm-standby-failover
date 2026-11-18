from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class Phase(Enum):
    """Coordinator state machine phases."""

    STEADY = auto()
    SUSPECT = auto()        # primary heartbeat stale, grace period running
    FENCING = auto()        # attempting to fence the old primary
    PROMOTING = auto()      # fenced, waiting for standby promotion ack
    FAILED_OVER = auto()    # standby promoted, new epoch active
    ROLLED_BACK = auto()    # promotion failed/timed out, routing restored


class Action(Enum):
    NONE = "none"
    WAIT = "wait"                 # keep waiting (grace period / lag may shrink)
    PROMOTE = "promote"           # decision: promotion may proceed
    REFUSE = "refuse"             # decision: promotion not safe/allowed
    ROLLBACK = "rollback"         # promotion attempt aborted, state restored


@dataclass(frozen=True)
class Heartbeat:
    node_id: str
    epoch: int
    durable_lsn: int   # highest position persisted on this node
    timestamp: float


@dataclass(frozen=True)
class ReplicationStatus:
    standby_id: str
    flush_lsn: int     # durably received by the standby
    replay_lsn: int    # applied by the standby
    lag_bytes: int
    lag_seconds: float


@dataclass(frozen=True)
class LossWindow:
    """Positions that may be lost: (recoverable, last_known_primary]."""

    recoverable_lsn: int          # clients can replay/recover from here
    primary_durable_lsn: int      # last position the old primary persisted

    @property
    def lost_operations(self) -> int:
        return max(0, self.primary_durable_lsn - self.recoverable_lsn)


@dataclass
class RoutingState:
    version: int
    primary_node: str
    epoch: int

    def snapshot(self) -> "RoutingState":
        return RoutingState(self.version, self.primary_node, self.epoch)


@dataclass(frozen=True)
class FailoverDecision:
    action: Action
    reason: str
    loss_window: Optional[LossWindow] = None
    routing: Optional[RoutingState] = None


@dataclass(frozen=True)
class RejoinReport:
    """What a recovered old primary must do before rejoining as standby."""

    accepted_as: str                     # always "standby"
    new_epoch: int                       # epoch it must adopt
    must_rewind_to: int                  # recoverable position
    divergent_window: Optional[LossWindow]  # acked-on-old-primary writes now lost
