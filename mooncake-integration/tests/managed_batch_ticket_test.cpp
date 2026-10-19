// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

// Hardware-free test of the production ticket/ownership state machine.
// Compile this file with g++ -std=c++20 -Wall -Wextra -Werror -pthread.
#include "../transfer_engine/managed_batch_ticket.h"

#include <cassert>
#include <iostream>

using mooncake::ManagedBatchPool;
using mooncake::ManagedBatchResult;
using mooncake::ManagedBatchState;

static void testCompletionRetiresOnce() {
    auto pool = std::make_shared<ManagedBatchPool>(1);
    auto descriptors = std::make_shared<int>(42);
    std::weak_ptr<int> retained = descriptors;
    int queries = 0;
    auto ticket = pool->create([descriptors, &queries]() {
        assert(*descriptors == 42);
        return ManagedBatchResult{++queries == 1 ? ManagedBatchState::WAITING
                                                 : ManagedBatchState::COMPLETED,
                                  ""};
    });
    descriptors.reset();
    assert(!retained.expired());
    assert(ticket->poll().state == ManagedBatchState::WAITING);
    assert(pool->size() == 1);
    assert(ticket->poll().state == ManagedBatchState::COMPLETED);
    assert(retained.expired());
    assert(pool->size() == 0);
    assert(ticket->poll().state == ManagedBatchState::COMPLETED);
    assert(queries == 2);
}

static void testFailureSurvivesGarbageCollection() {
    auto pool = std::make_shared<ManagedBatchPool>(1);
    auto descriptors = std::make_shared<int>(42);
    std::weak_ptr<int> retained = descriptors;
    auto ticket = pool->create([descriptors]() {
        return ManagedBatchResult{ManagedBatchState::FAILED_UNDRAINED,
                                  "route timeout"};
    });
    descriptors.reset();
    assert(ticket->poll().state == ManagedBatchState::FAILED_UNDRAINED);
    assert(ticket->error() == "route timeout");
    ticket.reset();
    assert(!retained.expired());
    assert(pool->size() == 1);
    bool rejected = false;
    try {
        pool->create([]() {
            return ManagedBatchResult{ManagedBatchState::COMPLETED, ""};
        });
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    assert(rejected);
    // Even dropping the engine's pool owner cannot release posted descriptors.
    pool.reset();
    assert(!retained.expired());
}

static void testPartialSubmitAndPreSubmitRejection() {
    auto pool = std::make_shared<ManagedBatchPool>(2);
    int queries = 0;
    auto make_ticket = [&]() {
        return pool->create([&queries]() {
            ++queries;
            return ManagedBatchResult{ManagedBatchState::WAITING, ""};
        });
    };
    auto unposted = make_ticket();
    unposted->rejectBeforeSubmit("peer not found");
    assert(pool->size() == 0);
    auto partial = make_ticket();
    partial->fail("submit failed after one transport accepted work");
    assert(partial->poll().state == ManagedBatchState::FAILED_UNDRAINED);
    assert(partial->poll().state == ManagedBatchState::FAILED_UNDRAINED);
    assert(pool->size() == 1);
    assert(queries == 0);
}

static void testPollExceptionIsQuarantined() {
    auto pool = std::make_shared<ManagedBatchPool>(1);
    int queries = 0;
    auto ticket = pool->create([&queries]() -> ManagedBatchResult {
        ++queries;
        throw std::runtime_error("status query failed");
    });
    assert(ticket->poll().state == ManagedBatchState::FAILED_UNDRAINED);
    assert(ticket->error() == "status query failed");
    assert(ticket->poll().state == ManagedBatchState::FAILED_UNDRAINED);
    assert(queries == 1);
    assert(pool->size() == 1);
}

static void testBoundedAdmissionConfiguration() {
    auto pool = std::make_shared<ManagedBatchPool>(2);
    auto ticket = pool->create(
        []() { return ManagedBatchResult{ManagedBatchState::COMPLETED, ""}; });
    pool->setLimit(1);
    bool rejected = false;
    try {
        pool->setLimit(0);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
    assert(ticket->poll().state == ManagedBatchState::COMPLETED);
    assert(pool->size() == 0);
}

int main() {
    testCompletionRetiresOnce();
    testFailureSurvivesGarbageCollection();
    testPartialSubmitAndPreSubmitRejection();
    testPollExceptionIsQuarantined();
    testBoundedAdmissionConfiguration();
    std::cout << "5 managed batch ticket cases passed\n";
}
