"""The broker-neutral contract suite (`tests/test_bus_contract.py`) over `SeededFaultBus` with faults on.

The suite was written for a bus that delivers exactly once in the exact dispatch order of S3. A networked bus gives
neither (`pluto/bus/faults.py`), so many contract tests fail when faults are injected; which ones, and why, is the
list of the guarantees a component must not build on. This module runs the suite with each fault that delivery can
suffer (publish failures are left out: the contract tests do not retry a failed publish, so they would all fail on
that) and
checks the list:

* a delay breaks exactly the tests that assert the order of S3 across lanes, no other test;
* duplicates (a copy published again, a lost ack, a consumer that restarts) break the tests that count deliveries and
  those that assume a replay or a position leaves nothing old to read;
* a lost delivery leaves the number of deliveries of a message alone and breaks the tests that count attempts;
* a set of tests survives every one of them: request and reply, close and cancel, argument checks, envelopes.

Run as `python -m tests.test_bus_faults_contract` it prints which tests fail under which fault, with the last line of
the failure. The suite is deterministic: the failing sets are those of fixed seeds.
"""

import sys
import unittest

from pluto.bus.faults import FaultConfig, SeededFaultBus
from tests.test_bus_contract import BusContract, make_memory, make_serializing

SEEDS = (0, 1, 2)

PROFILES = {
    "delay": FaultConfig(delay=0.5),
    "duplicate_publish": FaultConfig(duplicate_publish=0.5),
    "ack_lost": FaultConfig(ack_lost=0.5),
    "lose_delivery": FaultConfig(lose_delivery=0.5),
    "crash": FaultConfig(crash=0.5),
    "delivery": FaultConfig(delay=0.3, duplicate_publish=0.2, ack_lost=0.2, lose_delivery=0.2, crash=0.1),
}

# starts child interpreters for the unwrapped bus: nothing to do with the wrapper
EXCLUDED = {"test_delivery_trace_is_independent_of_hash_seed"}

# assert the order of S3 across lanes, topics or subscriptions, or that a replay repeats the order a live subscriber
# saw: a message that arrives late is dispatched late
ORDER = {
    "test_a_replay_delivers_in_the_order_a_live_subscriber_saw",
    "test_dispatch_does_not_depend_on_publish_order_across_lanes",
    "test_dispatch_is_across_topics_too",
    "test_dispatch_picks_lowest_logical_ts_then_producer_then_seq",
    "test_equal_content_falls_back_to_topic_then_key",
    "test_phase_orders_messages_of_the_same_logical_ts",
    "test_position_of_is_a_resume_point_without_gaps_when_keys_complete_out_of_log_order",
    "test_wildcards",
}

# count deliveries (each message once, in the handler) or expect an empty or exact replay
EXACTLY_ONCE = {
    "test_each_subscription_sees_the_whole_stream_once",
    "test_the_bus_does_not_deduplicate",
    "test_replay_from_earliest",
    "test_resume_from_the_position_of_a_delivery_continues_after_it",
    "test_a_position_at_the_end_of_the_log_waits_for_the_next_message",
}

# count the attempts of a message or the invocations of the handlers
ATTEMPTS = {
    "test_nack_redelivers_before_the_next_message_of_the_lane",
    "test_dead_letter_after_max_attempts_and_the_lane_moves_on",
    "test_exception_is_a_nack_and_a_redelivery",
    "test_run_until_idle_drains_cascades_and_counts_invocations",
}

# hold under every fault above: nothing in them depends on order, counts or attempts of deliveries
SURVIVORS = {
    "test_a_cancelled_request_ignores_its_reply",
    "test_a_closed_bus_refuses_every_call",
    "test_a_failing_on_reply_is_retried_by_the_bus",
    "test_a_handler_can_cancel_its_own_subscription",
    "test_a_late_duplicate_reply_is_ignored",
    "test_a_request_that_cannot_be_published_leaves_no_subscription",
    "test_ack_and_nack_apply_only_to_the_running_delivery",
    "test_an_empty_bus_is_idle",
    "test_cancel_after_close_is_a_no_op",
    "test_close_is_idempotent_and_final",
    "test_delivery_trace_is_repeatable",
    "test_envelope_fields_and_defaults",
    "test_envelopes_are_immutable",
    "test_group_members_must_read_the_same_stream",
    "test_logical_time_must_be_a_plain_int",
    "test_malformed_topics_are_refused",
    "test_max_deliveries_stops_a_handler_loop",
    "test_on_reply_is_called_from_the_dispatch_loop",
    "test_protobuf_payloads_arrive_equal",
    "test_rank_must_be_an_int",
    "test_replay_redelivers_with_attempt_one_and_the_original_envelope",
    "test_reply_carries_phase_and_schema_version",
    "test_request_gets_its_reply",
    "test_requests_do_not_see_each_others_replies",
    "test_requests_of_one_producer_share_its_reply_inbox",
    "test_the_reply_topic_must_be_an_exact_topic",
    "test_the_request_keeps_the_receipt_of_its_latest_send",
}

FACTORIES = {"memory": make_memory, "serializing": make_serializing}


def failing(profile: str) -> dict[str, str]:
    """The contract tests that fail under `profile`, on any implementation and seed, with the last line of a failure."""
    failures = {}
    for implementation, factory in FACTORIES.items():
        for seed in SEEDS:
            config = PROFILES[profile]

            def make_bus(*, factory=factory, seed=seed, config=config, **options):
                return SeededFaultBus(factory(**options), seed, config)

            case = type(
                "Contract",
                (BusContract, unittest.TestCase),
                {"implementation": implementation, "make_bus": staticmethod(make_bus)},
            )
            names = [n for n in unittest.defaultTestLoader.getTestCaseNames(case) if n not in EXCLUDED]
            result = unittest.TestResult()
            unittest.TestSuite(case(name) for name in names).run(result)
            for test, trace in result.failures + result.errors:
                failures.setdefault(test._testMethodName, trace.strip().splitlines()[-1][:160])
    return failures


class ContractUnderFaults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.failed = {profile: failing(profile) for profile in PROFILES}

    def test_a_delay_breaks_exactly_the_tests_of_the_dispatch_order(self):
        self.assertEqual(set(self.failed["delay"]), ORDER)

    def test_duplicates_break_the_tests_that_count_deliveries(self):
        for profile in ("duplicate_publish", "ack_lost", "crash"):
            self.assertLessEqual(EXACTLY_ONCE, set(self.failed[profile]), profile)
        for profile in ("duplicate_publish", "ack_lost"):  # a pattern subscription does not restart: no copies there
            self.assertIn("test_fifo_per_topic_and_key", self.failed[profile])

    def test_a_lost_delivery_leaves_the_count_alone_and_breaks_the_attempts(self):
        failed = set(self.failed["lose_delivery"])
        self.assertLessEqual(ATTEMPTS, failed)
        self.assertFalse(EXACTLY_ONCE & failed, EXACTLY_ONCE & failed)

    def test_ack_loss_breaks_the_attempts_too(self):
        self.assertLessEqual(ATTEMPTS, set(self.failed["ack_lost"]))

    def test_what_survives_every_fault(self):
        for profile, failed in self.failed.items():
            self.assertFalse(SURVIVORS & set(failed), profile)

    def test_every_survivor_and_order_test_is_a_contract_test(self):
        names = set(unittest.defaultTestLoader.getTestCaseNames(BusContract))
        self.assertLessEqual(SURVIVORS | ORDER | EXACTLY_ONCE | ATTEMPTS, names)


if __name__ == "__main__":
    for profile in PROFILES:
        failed = failing(profile)
        print(f"== {profile}: {len(failed)} contract tests fail")
        for name in sorted(failed):
            print(f"   {name}: {failed[name]}", file=sys.stdout)
