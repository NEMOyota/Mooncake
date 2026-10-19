---
orphan: true
---

# Managed asynchronous batch tickets

The experimental managed Python API supports bounded, incremental progress of
PD state transfers. It preserves request descriptors and the native engine while
work is pending or its completion is uncertain. Existing synchronous and raw
batch-ID APIs keep their original behavior.

```python
engine.set_managed_batch_limit(128)  # Per engine, default 128.
ticket = engine.batch_transfer_async_read_ticket(
    peer, local_destination_addresses, remote_source_addresses, lengths
)
state = ticket.poll()
```

`batch_transfer_async_write_ticket` has the same argument order: the second
argument contains local source addresses, and the third contains remote
destination addresses. Both methods accept the existing optional
`transport_hint` and `nic_hint` arguments. Address and length lists must be
nonempty and have equal sizes; every range must have positive length and a
nonzero, nonoverflowing address. Memory must already be registered appropriately.

`poll()` makes one native batch-status query, without a Python-binding busy loop
or sleep. It returns a string:

| Result | Contract |
| --- | --- |
| `WAITING` | Keep both endpoints' buffers, registration and request leases alive; poll again during a later progress turn. |
| `COMPLETED` | Every task completed, the aggregate transferred byte count matches the submitted ranges, and native cleanup was accepted; the ticket is retired and its admission slot is returned. Further polls return the same result. |
| `FAILED_UNDRAINED` | Submission, query, transfer, or cleanup failed. Partial publication or continued memory access cannot be excluded. Retain and isolate both endpoints' resources. Further polls return the same result. |

`ticket.error` contains a diagnostic string. A classic deferred cleanup is
conservatively reported as `FAILED_UNDRAINED`: its batch handle may already be
invalid, but request descriptors can still be referenced. This API never polls
that handle again. It does not automatically retry, reconnect, cancel, or fall
back to a synchronous transfer.

`engine.managed_batch_count` includes pending and quarantined tickets. The limit
is checked before peer lookup or transport publication. A full pool raises
`RuntimeError`; increasing its configured limit is an explicit operator choice,
and a new limit cannot be lower than current occupancy. Successful completion
returns capacity. Failed or abandoned tickets retain capacity, native request
descriptors and the engine for the life of the process. The intentional ownership
cycle prevents Python garbage collection from silently freeing posted requests.
Destroying the Python engine also retains its managed allocations while tickets
remain outstanding. Reinitialization is rejected while such tickets exist.

The API receives integer addresses, so it cannot retain application tensors or
remote cache blocks itself. The application must retain these objects and must
not unregister, free, overwrite, or reuse their storage before successful
completion. On failure it must quarantine the affected resources and arrange a
worker/process recovery that tears down the transport before reuse. Dropping a
ticket, a request deadline, or a terminal error is not proof of DMA quiescence.

Only the count of native managed batches is bounded here. A framework should
also bound descriptors and bytes per batch, total outstanding bytes, request
admission, and progress work per scheduling turn. Native peer discovery or
transport polling can itself perform work; this API does not promise a fixed
wall-clock bound for those underlying calls.

## PD integration

For cloud-prefill/edge-decode, the decode worker can use managed READ tickets
for reserved target blocks. Each ticket covers one bounded network batch. Only
after every required attention, recurrent and auxiliary state component has
completed may the connector publish its application COMMIT and make the request
decode-ready. Transfer completion does not validate a model/layout manifest,
advance a KV prefix index, or commit a multi-part state object by itself.

The implementation calls public `TransferEngine` operations and never casts a
batch ID to a classic transport structure. Both classic and TENT backends use
the same interface. Ascend hardware completion and WAN performance require
separate validation; no new HCCL, HCOMM or HIXL ABI is required.

## Focused verification

The ownership state machine can be built without a Mooncake installation:

```bash
g++ -std=c++20 -Wall -Wextra -Werror -pthread \
  mooncake-integration/tests/managed_batch_ticket_test.cpp \
  -o /tmp/mooncake-managed-batch-test
/tmp/mooncake-managed-batch-test
```

The five cases cover successful retirement, failure surviving garbage
collection, pre-submit rejection versus partial submission, query exceptions,
and bounded admission. This executes the same header used by the Python binding.

After building the native `engine` Python extension, run the real TCP smoke
test with the build's Python/library paths configured:

```bash
python mooncake-integration/tests/test_managed_batch_tcp.py
```

It checks full-batch WRITE and READ, remote byte visibility, repeat polling and
capacity return. Use the project's existing `MC_USE_TENT=1` TCP configuration to
exercise the TENT route separately. Neither native TCP nor Ascend hardware was
executed in the initial environment, which lacked the native build dependencies.
