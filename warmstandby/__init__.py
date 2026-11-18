"""Warm-standby failover coordinator.

Decides whether to promote a standby after primary loss, enforces
fencing-before-promotion, and gates writes on the current epoch so a
partitioned old primary cannot acknowledge writes that the new primary
will never see.
"""

from .models import (
    Action,
    FailoverDecision,
    Heartbeat,
    LossWindow,
    Phase,
    RejoinReport,
    ReplicationStatus,
    RoutingState,
)
from .coordinator import FailoverCoordinator
from .fencing import FenceTokenStore

__all__ = [
    "Action",
    "FailoverCoordinator",
    "FailoverDecision",
    "FenceTokenStore",
    "Heartbeat",
    "LossWindow",
    "Phase",
    "RejoinReport",
    "ReplicationStatus",
    "RoutingState",
]
