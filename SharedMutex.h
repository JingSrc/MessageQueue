#pragma once

#include <mutex>

#if __cplusplus < 201703L

#include <condition_variable>

class shared_mutex
{
public:
    shared_mutex() = default;
    ~shared_mutex() = default;

    shared_mutex(const shared_mutex &) = delete;
    shared_mutex& operator=(const shared_mutex &) = delete;
    shared_mutex(const shared_mutex &&) = delete;
    shared_mutex& operator=(const shared_mutex &&) = delete;

    void lock_shared() {
        std::unique_lock<std::mutex> lock(mutex_ );
        read_cond_.wait(lock, [this]()-> bool {
            return write_count_ == 0;
        });
        ++read_count_;
    }

    void unlock_shared() {
        std::unique_lock<std::mutex> lock(mutex_ );
        if (--read_count_ == 0 && write_count_ > 0) {
            write_cond_.notify_one();
        }
    }

    void lock() {
        std::unique_lock<std::mutex> lock(mutex_ );
        ++write_count_;
        write_cond_.wait(lock, [this]()-> bool {
            return read_count_ == 0 && !writing_;
        });
        writing_ = true;
    }

    void unlock() {
        std::unique_lock<std::mutex> lock(mutex_ );
        if (--write_count_ == 0) {
            read_cond_.notify_all();
        } else {
            write_cond_.notify_one();
        }
        writing_ = false;
    }

private:
    volatile size_t read_count_ = 0;
    volatile size_t write_count_ = 0;
    volatile bool writing_ = false;
    std::mutex mutex_;
    std::condition_variable read_cond_;
    std::condition_variable write_cond_;
};

template<typename _shared_mutex>
class shared_lock
{
public:
    explicit shared_lock(_shared_mutex& rw_mutex)
        : rw_mutex_(&rw_mutex) {
        rw_mutex_->lock_shared();
    }

    ~shared_lock() {
        if (rw_mutex_) {
            rw_mutex_->unlock_shared();
        }
    }

    shared_lock() = delete;
    shared_lock(const shared_lock &) = delete;
    shared_lock & operator = (const shared_lock &) = delete;
    shared_lock(const shared_lock &&) = delete;
    shared_lock & operator = (const shared_lock &&) = delete;

private:
    _shared_mutex *rw_mutex_ = nullptr;
};

#else

#include <shared_mutex>

template<typename _shared_mutex>
using shared_lock = std::shared_lock<_shared_mutex>;

using shared_mutex = std::shared_mutex;

#endif

template<typename _shared_mutex>
using unique_lock = std::unique_lock<_shared_mutex>;
