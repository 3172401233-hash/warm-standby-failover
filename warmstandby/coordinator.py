from __future__ import annotations

from typing import List, Optional, Tuple

from .fencing import FenceTokenStore
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


class FailoverCoordinator:
    """Drives warm-standby promotion for one primary/standby pair.

    Safety order: suspect -> fence old primary -> promote standby ->
    cut over routing with a bumped epoch. Writes are only accepted when
    they carry the current epoch and no transition is in flight, so a
    partitioned old primary can never keep acknowledging writes after
    the standby has taken over.
    """

    def __init__(
        self,
        primary_id: str,
        standby_id: str,
        fence_store: FenceTokenStore,
        *,
        heartbeat_timeout: float = 10.0,
        promote_timeout: float = 30.0,
        max_allowed_loss: int = 0,
        epoch: int = 1,
        routing_version: int = 1,
    ) -> None:
        self.primary_id = primary_id
        self.standby_id = standby_id
        self.fence_store = fence_store
        self.heartbeat_timeout = heartbeat_timeout
        self.promote_timeout = promote_timeout
        self.max_allowed_loss = max_allowed_loss

        self.routing = RoutingState(routing_version, primary_id, epoch)
        self.phase = Phase.STEADY

        self._last_primary_hb: Optional[Heartbeat] = None
        self._replication: Optional[ReplicationStatus] = None
        self._suspect_since: Optional[float] = None
        self._promote_deadline: Optional[float] = None
        self._fence_token: Optional[int] = None
        self._pre_promotion_routing: Optional[RoutingState] = None
        self._recoverable_lsn: Optional[int] = None
        self._loss_window: Optional[LossWindow] = None

        self.events: List[Tuple[float, str]] = []

    # ---- observations -------------------------------------------------

    def observe_heartbeat(self, hb: Heartbeat) -> None:
        if hb.node_id == self.routing.primary_node:
            self._last_primary_hb = hb

    def observe_replication(self, status: ReplicationStatus) -> None:
        if status.standby_id == self.standby_id:
            self._replication = status

    # ---- state machine --------------------------------------------------

    def tick(self, now: float) -> FailoverDecision:
        if self.phase == Phase.STEADY:
            return self._tick_steady(now)
        if self.phase == Phase.SUSPECT:
            return self._tick_suspect(now)
        if self.phase == Phase.PROMOTING:
            return self._tick_promoting(now)
        return FailoverDecision(Action.NONE, f"phase={self.phase.name}",
                                routing=self.routing)

    def _tick_steady(self, now: float) -> FailoverDecision:
        if self._primary_stale(now):
            self.phase = Phase.SUSPECT
            self._suspect_since = now
            self._log(now, "primary heartbeat stale, entering suspect")
            return FailoverDecision(Action.WAIT, "primary suspect, grace period",
                                    routing=self.routing)
        return FailoverDecision(Action.NONE, "primary healthy", routing=self.routing)

    def _tick_suspect(self, now: float) -> FailoverDecision:
        if not self._primary_stale(now):
            self.phase = Phase.STEADY
            self._suspect_since = None
            self._log(now, "primary recovered during grace period")
            return FailoverDecision(Action.NONE, "primary recovered",
                                    routing=self.routing)
        assert self._suspect_since is not None
        if now - self._suspect_since < self.heartbeat_timeout:
            return FailoverDecision(Action.WAIT, "within heartbeat grace period",
                                    routing=self.routing)
        return self._begin_promotion(now)

    def _begin_promotion(self, now: float, force: bool = False) -> FailoverDecision:
        window = self.loss_window()
        if (
            not force
            and window is not None
            and window.lost_operations > self.max_allowed_loss
        ):
            self._log(now, f"refusing promotion: loss {window.lost_operations} "
                           f"> allowed {self.max_allowed_loss}")
            return FailoverDecision(
                Action.REFUSE,
                "replication lag exceeds allowed data loss",
                loss_window=window,
                routing=self.routing,
            )

        self.phase = Phase.FENCING
        self._fence_token = self.fence_store.acquire(self.standby_id)
        if self._fence_token is None or not self.fence_store.is_fenced(self.primary_id):
            # Could not establish exclusive fencing; never promote.
            self.phase = Phase.SUSPECT
            self._log(now, "fencing failed, aborting promotion")
            return FailoverDecision(Action.REFUSE, "old primary not fenced",
                                    loss_window=window, routing=self.routing)

        self._pre_promotion_routing = self.routing.snapshot()
        self._recoverable_lsn = (
            self._replication.flush_lsn if self._replication else 0
        )
        self._loss_window = window
        self.phase = Phase.PROMOTING
        self._promote_deadline = now + self.promote_timeout
        self._log(now, f"old primary fenced (token={self._fence_token}), "
                       f"promoting {self.standby_id}")
        return FailoverDecision(Action.PROMOTE, "fenced, promotion started",
                                loss_window=window, routing=self.routing)

    def _tick_promoting(self, now: float) -> FailoverDecision:
        assert self._promote_deadline is not None
        if now <= self._promote_deadline:
            return FailoverDecision(Action.WAIT, "awaiting promotion ack",
                                    loss_window=self._loss_window,
                                    routing=self.routing)
        return self._rollback(now, "promotion timed out")

    def confirm_promotion(self, now: float) -> FailoverDecision:
        """Standby calls this once it has promoted itself."""
        if self.phase != Phase.PROMOTING:
            return FailoverDecision(Action.REFUSE, "no promotion in flight",
                                    routing=self.routing)
        assert self._promote_deadline is not None
        if now > self._promote_deadline:
            return self._rollback(now, "promotion ack after deadline")
        # Re-validate fencing before committing the cutover: a racing
        # coordinator may have evicted our token since we acquired it.
        if not self.fence_store.write_permitted(self.standby_id,
                                                self._fence_token or -1):
            return self._rollback(now, "fencing token lost during promotion")

        self.routing = RoutingState(
            version=self.routing.version + 1,
            primary_node=self.standby_id,
            epoch=self.routing.epoch + 1,
        )
        self.phase = Phase.FAILED_OVER
        self._log(now, f"cutover complete: epoch={self.routing.epoch} "
                       f"routing_version={self.routing.version}")
        return FailoverDecision(Action.PROMOTE, "failover committed",
                                loss_window=self._loss_window,
                                routing=self.routing)

    def _rollback(self, now: float, reason: str) -> FailoverDecision:
        if self._pre_promotion_routing is not None:
            self.routing = self._pre_promotion_routing
        if self._fence_token is not None:
            self.fence_store.release(self.standby_id, self._fence_token)
        self._fence_token = None
        self._promote_deadline = None
        self.phase = Phase.ROLLED_BACK
        self._log(now, f"rollback: {reason}")
        return FailoverDecision(Action.ROLLBACK, reason,
                                loss_window=self._loss_window,
                                routing=self.routing)

    # ---- write gating ---------------------------------------------------

    def accept_write(self, epoch: int, now: float) -> bool:
        """Client-facing gate: only current-epoch writes outside transitions."""
        if self.phase not in (Phase.STEADY, Phase.FAILED_OVER):
            return False
        return epoch == self.routing.epoch

    def storage_write_permitted(self, node_id: str, token: int) -> bool:
        """Storage-side gate: the fenced old primary cannot persist."""
        return self.fence_store.write_permitted(node_id, token)

    # ---- old primary rejoin ---------------------------------------------

    def rejoin(self, hb: Heartbeat) -> RejoinReport:
        """A recovered old primary must rewind and adopt the new epoch."""
        recoverable = self._recoverable_lsn or 0
        divergent = None
        if hb.durable_lsn > recoverable:
            divergent = LossWindow(recoverable, hb.durable_lsn)
        self._log(hb.timestamp,
                  f"old primary rejoining as standby, rewind to {recoverable}")
        return RejoinReport(
            accepted_as="standby",
            new_epoch=self.routing.epoch,
            must_rewind_to=recoverable,
            divergent_window=divergent,
        )

    # ---- reporting --------------------------------------------------------

    def loss_window(self) -> Optional[LossWindow]:
        if self._last_primary_hb is None or self._replication is None:
            return None
        return LossWindow(
            recoverable_lsn=self._replication.flush_lsn,
            primary_durable_lsn=self._last_primary_hb.durable_lsn,
        )

    @property
    def recoverable_lsn(self) -> Optional[int]:
        return self._recoverable_lsn

    def _primary_stale(self, now: float) -> bool:
        if self._last_primary_hb is None:
            return True
        return now - self._last_primary_hb.timestamp > self.heartbeat_timeout

    def _log(self, now: float, message: str) -> None:
        self.events.append((now, message))
