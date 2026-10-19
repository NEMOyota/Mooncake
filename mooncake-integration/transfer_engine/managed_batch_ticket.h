// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

namespace mooncake {

// No transport-specific handle is inspected here. The same lifetime machinery
// is used by the Python binding and the hardware-free unit test.
enum class ManagedBatchState { WAITING, COMPLETED, FAILED_UNDRAINED };

struct ManagedBatchResult {
    ManagedBatchState state;
    std::string error;
};

class ManagedBatchPool;

class ManagedBatchTicket {
   public:
    ManagedBatchResult poll();

    std::string error() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_.error;
    }

    // Called only by the submitter before handing the ticket to Python.
    void fail(std::string error) {
        std::lock_guard<std::mutex> lock(mutex_);
        result_ = {ManagedBatchState::FAILED_UNDRAINED, std::move(error)};
    }

    // Admission/allocation failed before any transport submit was attempted.
    // Unlike fail(), this proves that no work owns the descriptors.
    void rejectBeforeSubmit(std::string error);

   private:
    friend class ManagedBatchPool;
    ManagedBatchTicket(std::shared_ptr<ManagedBatchPool> owner,
                       std::function<ManagedBatchResult()> poll)
        : owner_(std::move(owner)), poll_(std::move(poll)) {}

    mutable std::mutex mutex_;
    std::shared_ptr<ManagedBatchPool> owner_;
    std::function<ManagedBatchResult()> poll_;
    ManagedBatchResult result_{ManagedBatchState::WAITING, ""};
};

class ManagedBatchPool : public std::enable_shared_from_this<ManagedBatchPool> {
   public:
    explicit ManagedBatchPool(size_t limit = 128) : limit_(limit) {
        if (limit == 0)
            throw std::invalid_argument("batch limit must be positive");
    }

    std::shared_ptr<ManagedBatchTicket> create(
        std::function<ManagedBatchResult()> poll) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (tickets_.size() >= limit_) {
            throw std::runtime_error(
                "managed batch limit reached; pending or quarantined transfers "
                "still own capacity");
        }
        auto ticket = std::shared_ptr<ManagedBatchTicket>(
            new ManagedBatchTicket(shared_from_this(), std::move(poll)));
        tickets_.insert(ticket);
        return ticket;
    }

    void setLimit(size_t limit) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (limit == 0 || limit < tickets_.size()) {
            throw std::invalid_argument(
                "batch limit must be positive and cover outstanding tickets");
        }
        limit_ = limit;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tickets_.size();
    }

   private:
    friend class ManagedBatchTicket;
    void retire(ManagedBatchTicket* ticket) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = tickets_.begin(); it != tickets_.end(); ++it) {
            if (it->get() == ticket) {
                tickets_.erase(it);
                return;
            }
        }
    }

    mutable std::mutex mutex_;
    size_t limit_;
    // The pool/ticket ownership cycle is intentional until successful retire.
    // Dropping a Python ticket or engine must not destroy an in-flight request
    // vector. Failed/unpolled tickets remain bounded by limit_ for process
    // life.
    std::unordered_set<std::shared_ptr<ManagedBatchTicket>> tickets_;
};

inline ManagedBatchResult ManagedBatchTicket::poll() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (result_.state != ManagedBatchState::WAITING) return result_;
    try {
        result_ = poll_();
    } catch (const std::exception& exc) {
        result_ = {ManagedBatchState::FAILED_UNDRAINED, exc.what()};
    } catch (...) {
        result_ = {ManagedBatchState::FAILED_UNDRAINED,
                   "unknown exception while polling transfer"};
    }
    if (result_.state == ManagedBatchState::COMPLETED) {
        poll_ = {};
        owner_->retire(this);
    }
    return result_;
}

inline void ManagedBatchTicket::rejectBeforeSubmit(std::string error) {
    std::lock_guard<std::mutex> lock(mutex_);
    result_ = {ManagedBatchState::FAILED_UNDRAINED, std::move(error)};
    poll_ = {};
    owner_->retire(this);
}

inline const char* managedBatchStateName(ManagedBatchState state) {
    switch (state) {
        case ManagedBatchState::WAITING:
            return "WAITING";
        case ManagedBatchState::COMPLETED:
            return "COMPLETED";
        case ManagedBatchState::FAILED_UNDRAINED:
            return "FAILED_UNDRAINED";
    }
    return "FAILED_UNDRAINED";
}

}  // namespace mooncake
