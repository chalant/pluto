"""`Receipt`: the position is known at once on the in-memory bus and may be resolved later on a networked one."""
import unittest

from pluto.bus import BusError, InMemoryBus, Position, Receipt


class ReceiptTests(unittest.TestCase):
    def test_a_known_position_is_resolved(self):
        receipt = Receipt('m', Position('t', 3))

        self.assertTrue(receipt.resolved)
        self.assertEqual(receipt.position, Position('t', 3))
        self.assertEqual(receipt.msg_id, 'm')

    def test_a_position_given_as_a_function_is_resolved_when_asked_for_and_only_once(self):
        calls = []

        def resolve():
            calls.append(1)
            return Position('t', 7)

        receipt = Receipt('m', resolve)

        self.assertFalse(receipt.resolved)
        self.assertEqual(calls, [])
        self.assertIn('unresolved', repr(receipt))
        self.assertEqual(receipt.position, Position('t', 7))
        self.assertEqual(receipt.position, Position('t', 7))
        self.assertTrue(receipt.resolved)
        self.assertEqual(len(calls), 1)

    def test_a_failed_resolution_raises_again_on_the_next_ask(self):
        def resolve():
            raise BusError('the broker refused the message')

        receipt = Receipt('m', resolve)

        for _ in range(2):
            with self.assertRaises(BusError):
                receipt.position
        self.assertFalse(receipt.resolved)

    def test_the_in_memory_bus_returns_resolved_receipts(self):
        bus = InMemoryBus()
        receipt = bus.publish('t', 1, type='t', logical_ts=0, producer_id='p')

        self.assertTrue(receipt.resolved)
        self.assertEqual(receipt.position, Position('t', 0))


if __name__ == '__main__':
    unittest.main()
