import unittest

from warmstandby import (
    Action,
    FailoverCoordinator,
    FenceTokenStore,
    Heartbeat,
    Phase,
    ReplicationStatus,
)


def hb(node, epoch, lsn, ts):
    return Heartbeat(node_id=node, epoch=epoch, durable_lsn=lsn, timestamp=ts)


def repl(standby, flush, replay, lag_bytes=0, lag_seconds=0.0):
    return ReplicationStatus(
        standby_id=standby, flush_lsn=flush, replay_lsn=replay,
        lag_bytes=lag_bytes, lag_seconds=lag_seconds,
    )


def make_coordinator(store=None, **kwargs):
    store = store or FenceTokenStore()
    defaults = dict(heartbeat_timeout=10.0, promote_timeout=30.0,
                    max_allowed_loss=100)
    defaults.update(kwargs)
    return FailoverCoordinator("primary-a", "standby-b", store, **defaults), store


def drive_to_promoting(coord, now=100.0, primary_lsn=1000, standby_flush=990):
    """Feed a dead-primary view and run ticks until promotion starts."""
    coord.observe_heartbeat(hb("primary-a", 1, primary_lsn, ts=0.0))
    coord.observe_replication(repl("standby-b", standby_flush, standby_flush))
    coord.tick(now)                       # STEADY -> SUSPECT
    return coord.tick(now + 11.0)         # grace elapsed -> PROMOTE


class BidirectionalPartitionTest(unittest.TestCase):
    def test_only_one_side_promotes(self):
        # Both sites lose sight of each other and both try to fail over
        # to their local node against one shared fencing store.
        store = FenceTokenStore()
        coord_a = FailoverCoordinator("site-b", "site-a", store,
                                      heartbeat_timeout=10.0)
        coord_b = FailoverCoordinator("site-a", "site-b", store,
                                      heartbeat_timeout=10.0)
        for coord, peer, local in ((coord_a, "site-b", "site-a"),
                                   (coord_b, "site-a", "site-b")):
            coord.observe_heartbeat(hb(peer, 1, 500, ts=0.0))
            coord.observe_replication(repl(local, 500, 500))
            coord.tick(100.0)
            decision = coord.tick(111.0)
            self.assertEqual(decision.action, Action.PROMOTE)
            self.assertEqual(coord.phase, Phase.PROMOTING)

        # Interleaved: A fenced first, B's fence evicted A's token.
        decision_a = coord_a.confirm_promotion(120.0)
        decision_b = coord_b.confirm_promotion(120.0)

        self.assertEqual(decision_a.action, Action.ROLLBACK)
        self.assertEqual(coord_a.phase, Phase.ROLLED_BACK)
        self.assertEqual(decision_b.action, Action.PROMOTE)
        self.assertEqual(coord_b.phase, Phase.FAILED_OVER)

        # Storage must reject writes from the fenced loser.
        self.assertFalse(store.write_permitted("site-a", 1))
        self.assertTrue(store.write_permitted("site-b", store.holder and 2))


class OldPrimaryRejoinTest(unittest.TestCase):
    def test_rejoin_reports_divergence_and_rewind_point(self):
        coord, _ = make_coordinator()
        drive_to_promoting(coord)
        coord.confirm_promotion(115.0)
        self.assertEqual(coord.phase, Phase.FAILED_OVER)

        # Old primary comes back with writes the new primary never saw.
        report = coord.rejoin(hb("primary-a", 1, 1050, ts=200.0))
        self.assertEqual(report.accepted_as, "standby")
        self.assertEqual(report.new_epoch, 2)
        self.assertEqual(report.must_rewind_to, 990)
        self.assertIsNotNone(report.divergent_window)
        self.assertEqual(report.divergent_window.lost_operations, 60)
        self.assertEqual(report.divergent_window.recoverable_lsn, 990)

    def test_old_epoch_writes_rejected_after_cutover(self):
        coord, _ = make_coordinator()
        drive_to_promoting(coord)
        coord.confirm_promotion(115.0)
        self.assertFalse(coord.accept_write(epoch=1, now=120.0))
        self.assertTrue(coord.accept_write(epoch=2, now=120.0))


class ReplicationLagTest(unittest.TestCase):
    def test_lag_beyond_allowed_loss_refuses_promotion(self):
        coord, _ = make_coordinator(max_allowed_loss=100)
        decision = drive_to_promoting(coord, primary_lsn=1000, standby_flush=500)
        self.assertEqual(decision.action, Action.REFUSE)
        self.assertIn("lag", decision.reason)
        self.assertEqual(decision.loss_window.lost_operations, 500)
        self.assertEqual(decision.loss_window.recoverable_lsn, 500)
        self.assertEqual(coord.phase, Phase.SUSPECT)

    def test_lag_within_allowed_loss_promotes(self):
        coord, _ = make_coordinator(max_allowed_loss=100)
        decision = drive_to_promoting(coord, primary_lsn=1000, standby_flush=950)
        self.assertEqual(decision.action, Action.PROMOTE)
        self.assertEqual(decision.loss_window.lost_operations, 50)

    def test_recoverable_position_after_failover(self):
        coord, _ = make_coordinator()
        drive_to_promoting(coord, primary_lsn=1000, standby_flush=990)
        decision = coord.confirm_promotion(115.0)
        self.assertEqual(coord.recoverable_lsn, 990)
        self.assertEqual(decision.loss_window.primary_durable_lsn, 1000)
        self.assertEqual(decision.loss_window.lost_operations, 10)


class PromotionTimeoutTest(unittest.TestCase):
    def test_timeout_triggers_rollback(self):
        coord, store = make_coordinator(promote_timeout=30.0)
        drive_to_promoting(coord)
        self.assertEqual(coord.phase, Phase.PROMOTING)

        decision = coord.tick(100.0 + 11.0 + 31.0)  # past the deadline
        self.assertEqual(decision.action, Action.ROLLBACK)
        self.assertEqual(coord.phase, Phase.ROLLED_BACK)

    def test_late_ack_after_deadline_rolls_back(self):
        coord, _ = make_coordinator(promote_timeout=30.0)
        drive_to_promoting(coord)
        decision = coord.confirm_promotion(100.0 + 11.0 + 31.0)
        self.assertEqual(decision.action, Action.ROLLBACK)


class RollbackTest(unittest.TestCase):
    def test_rollback_restores_routing_and_epoch(self):
        coord, store = make_coordinator(promote_timeout=30.0)
        drive_to_promoting(coord)
        coord.tick(100.0 + 11.0 + 31.0)

        self.assertEqual(coord.routing.primary_node, "primary-a")
        self.assertEqual(coord.routing.epoch, 1)
        self.assertEqual(coord.routing.version, 1)
        self.assertIsNone(store.holder)  # fencing token released

    def test_rollback_on_fence_token_lost(self):
        store = FenceTokenStore()
        coord, _ = make_coordinator(store=store)
        drive_to_promoting(coord)
        store.acquire("intruder")  # a third party evicts our token
        decision = coord.confirm_promotion(115.0)
        self.assertEqual(decision.action, Action.ROLLBACK)
        self.assertEqual(coord.routing.epoch, 1)

    def test_writes_rejected_during_transition(self):
        coord, _ = make_coordinator()
        drive_to_promoting(coord)
        self.assertFalse(coord.accept_write(epoch=1, now=112.0))
        self.assertFalse(coord.accept_write(epoch=2, now=112.0))


if __name__ == "__main__":
    unittest.main()
