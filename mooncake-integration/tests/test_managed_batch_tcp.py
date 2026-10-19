"""Optional real classic/TENT TCP smoke test against the built engine module."""

import ctypes
import time
import unittest

import engine

RESOURCE_QUARANTINE = []


class ManagedBatchTCPTest(unittest.TestCase):
    def test_write_and_read_completion(self):
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()
        for te in (target, initiator):
            self.assertEqual(te.initialize("127.0.0.1", "P2PHANDSHAKE", "tcp", ""), 0)
            te.set_managed_batch_limit(2)

        remote = ctypes.create_string_buffer(128)
        source = ctypes.create_string_buffer(b"managed PD state", 128)
        local = ctypes.create_string_buffer(128)
        # Failed tickets cannot prove transport quiescence. Keep the actual
        # buffers/engines alive if any assertion below fails.
        RESOURCE_QUARANTINE.append((target, initiator, remote, source, local))
        target_addr = ctypes.addressof(remote)
        source_addr = ctypes.addressof(source)
        local_addr = ctypes.addressof(local)
        self.assertEqual(target.register_memory(target_addr, 128), 0)
        self.assertEqual(initiator.register_memory(source_addr, 128), 0)
        self.assertEqual(initiator.register_memory(local_addr, 128), 0)
        peer = f"127.0.0.1:{target.get_rpc_port()}"

        # Two noncontiguous ranges exercise whole-batch completion, not task 0.
        for method, local_base in (
            (initiator.batch_transfer_async_write_ticket, source_addr),
            (initiator.batch_transfer_async_read_ticket, local_addr),
        ):
            ticket = method(
                peer,
                [local_base, local_base + 64],
                [target_addr, target_addr + 64],
                [64, 64],
            )
            deadline = time.monotonic() + 10
            while (state := ticket.poll()) == "WAITING":
                if time.monotonic() >= deadline:
                    # Preserve all Python allocations on failure as a real
                    # connector must. This smoke runs in its own process.
                    self.fail("transfer remained pending; restart test process")
                time.sleep(0.001)
            self.assertEqual(state, "COMPLETED", ticket.error)
            self.assertEqual(ticket.poll(), "COMPLETED")
            self.assertEqual(initiator.managed_batch_count, 0)

        self.assertEqual(source.raw, remote.raw)
        self.assertEqual(source.raw, local.raw)
        for te, address in (
            (target, target_addr),
            (initiator, source_addr),
            (initiator, local_addr),
        ):
            self.assertEqual(te.unregister_memory(address), 0)
        RESOURCE_QUARANTINE.clear()


if __name__ == "__main__":
    unittest.main()
