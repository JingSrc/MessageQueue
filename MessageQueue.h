#pragma once

#include <mutex>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <atomic>
#include <future>
#include <queue>
#include <list>
#include <tuple>
#include <typeindex>
#include <functional>
#include <iostream>
#include <condition_variable>

#if __cplusplus < 201703L
namespace std
{
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
            std::unique_lock<std::mutex> lock(mutex_);
            read_cond_.wait(lock, [this] { return write_count_ == 0; });
            ++read_count_;
        }

        void unlock_shared() {
            std::unique_lock<std::mutex> lock(mutex_);
            if (--read_count_ == 0 && write_count_ > 0) {
                write_cond_.notify_one();
            }
        }

        void lock() {
            std::unique_lock<std::mutex> lock(mutex_);
            ++write_count_;
            write_cond_.wait(lock, [this] { return read_count_ == 0 && !writing_; });
            writing_ = true;
        }

        void unlock() {
            std::unique_lock<std::mutex> lock(mutex_);
            if (--write_count_ == 0) {
                read_cond_.notify_all();
            }
            else {
                write_cond_.notify_one();
            }
            writing_ = false;
        }

    private:
        std::mutex mutex_;
        std::condition_variable read_cond_;
        std::condition_variable write_cond_;
        volatile size_t read_count_{ 0 };
        volatile size_t write_count_{ 0 };
        volatile bool writing_{ false };
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

    class any
    {
        template <typename T> using decay_type = typename std::decay<T>::type;
    public:
        any() = default;
        any(const any &other) { *this = other; }
        any(any &&other) { *this = std::move(other); }

        template <typename T, typename std::enable_if<!std::is_same<decay_type<T>, any>::value, bool>::type = true>
        any(T &&data) {
            data_.reset(new internal_data_impl<decay_type<T>>{ std::forward<T>(data) });
        }

        any(const char *data) {
            data_.reset(new internal_data_impl<std::string>{ data });
        }

        ~any() = default;

        any &operator=(const any &other) {
            if (this != &other) {
                data_ = other.data_->clone();
            }
            return *this;
        }

        any &operator=(any &&other) {
            if (this != &other) {
                data_ = std::move(other.data_);
            }
            return *this;
        }

        template <typename T, typename std::enable_if<!std::is_same<decay_type<T>, any>::value, bool>::type = true>
        any &operator=(T &&data) {
            data_.reset(new internal_data_impl<decay_type<T>>{ std::forward<T>(data) });
            return *this;
        }

        const std::type_info &type() const {
            return (!has_value()) ? data_->type() : typeid(void);
        }
        bool has_value() const { return data_ == nullptr; }

        template <typename T>
        const T &value() const {
            return static_cast<const internal_data_impl<T> *>(data_.get())->data_;
        }
        template <typename T>
        T &value() {
            return static_cast<internal_data_impl<T> *>(data_.get())->data_;
        }

    private:
        class internal_data
        {
        public:
            internal_data() = default;
            virtual ~internal_data() = default;

            virtual const std::type_info &type() const = 0;
            virtual std::unique_ptr<internal_data> clone() const = 0;
        };

        template <typename T>
        class internal_data_impl : public internal_data
        {
        public:
            internal_data_impl(const T &data)
                : data_{ data } {}
            internal_data_impl(T &&data)
                : data_{ std::move(data) } {}

            virtual const std::type_info &type() const override { return typeid(T); }
            virtual std::unique_ptr<internal_data> clone() const override {
                return std::unique_ptr<internal_data_impl>(new internal_data_impl<T>(data_));
            }

            T data_;
        };

    private:
        std::unique_ptr<internal_data> data_;
    };

    template<typename T>
    const T &any_cast(const any &a) {
        return a.value<T>();
    }

    template<typename T>
    T &any_cast(any &a) {
        return a.value<T>();
    }
}
#else
#include <shared_mutex>
#endif

class thread_pool_t
{
public:
    explicit thread_pool_t(size_t threads = std::thread::hardware_concurrency()) {
        for (size_t i = 0; i < threads; ++i) {
            workers_.push_back(std::make_shared<thread_worker>());
        }
        groups_.resize(threads);
    }
    ~thread_pool_t() = default;

    thread_pool_t(const thread_pool_t &) = delete;
    thread_pool_t& operator=(const thread_pool_t &) = delete;
    thread_pool_t(const thread_pool_t &&) = delete;
    thread_pool_t& operator=(const thread_pool_t &&) = delete;

    inline int indices() const { return static_cast<int>(workers_.size()); }

    template<class F, class... Args>
    void push(F&& f, Args&&... args) {
        auto idleIt = workers_.begin();
        for (auto it = idleIt; it != workers_.end(); ++it) {
            auto busy = (*it)->task_size();
            if (busy == 0) {
                idleIt = it;
                break;
            }
            if (busy < (*idleIt)->task_size()) {
                idleIt = it;
            }
        }

        (*idleIt)->push(f, std::forward<Args>(args)...);
    }

    template<class F, class... Args>
    void push_to_thread(size_t idx, F&& f, Args&&... args) {
        workers_[idx]->push(f, std::forward<Args>(args)...);
    }

    template<class F, class... Args>
    void push_to_group(const std::string &group, F&& f, Args&&... args) {
        std::shared_lock<std::shared_mutex> lock{ mutex_ };
        for (size_t i = 0; i < groups_.size(); ++i) {
            auto &grp = groups_[i];
            if (grp.find(group) != grp.end()) {
                workers_[i]->push(f, std::forward<Args>(args)...);
                break;
            }
        }
    }

    std::vector<std::string> groups() const {
        std::shared_lock<std::shared_mutex> lock{ mutex_ };

        std::vector<std::string> grps;
        for (size_t i = 0; i < groups_.size(); ++i) {
            auto &grp = groups_[i];
            grps.insert(grps.end(), grp.begin(), grp.end());
        }
        return grps;
    }

    void add_group(const std::string &group) {
        std::unique_lock<std::shared_mutex> lock{ mutex_ };

        auto groupIt = groups_.begin();
        auto groupSize = groupIt->size();
        for (auto it = groupIt; it != groups_.end(); ++it) {
            if (it->find(group) != it->end()) {
                groupIt = groups_.end();
                break;
            }

            auto size = (*it).size();
            if (size < groupSize) {
                groupIt = it;
                groupSize = size;
            }
        }

        if (groupIt != groups_.end()) {
            groupIt->insert(group);
        }
    }

    void remove_group(const std::string &group) {
        std::unique_lock<std::shared_mutex> lock{ mutex_ };
        for (auto gIt = groups_.begin(); gIt != groups_.end(); ++gIt) {
            auto it = gIt->find(group);
            if (it != gIt->end()) {
                gIt->erase(it);
            }
        }
    }

private:
    class thread_worker
    {
    public:
        explicit thread_worker() {
            std::thread thd{ &thread_worker::run, this };
            thread_ = std::move(thd);
        }
        ~thread_worker() {
            stop_ = true;
            cond_.notify_all();
            thread_.join();
        }

        inline int task_size() const {
            return task_size_;
        }

        template<class F, class... Args>
        void push(F&& f, Args&&... args) {
            auto task = std::make_shared<std::packaged_task<void()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
            {
                std::unique_lock<std::mutex> lock{ mutex_ };
                if (stop_) {
                    throw std::runtime_error("enqueue on stopped Worker");
                }

                task_size_++;
                tasks_.emplace([task]() { (*task)(); });
            }

            cond_.notify_one();
        }

    private:
        void run() {
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock{ mutex_ };
                    cond_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                    if (stop_ && tasks_.empty()) {
                        return;
                    }
                    task = std::move(tasks_.front());
                    task_size_--;
                    tasks_.pop();
                }

                task();
            }
        }

    private:
        std::thread thread_;
        mutable std::mutex mutex_;
        std::atomic_int task_size_{ 0 };
        std::queue<std::function<void()>> tasks_;
        std::condition_variable cond_;
        std::atomic_bool stop_{ false };
    };

private:
    std::vector<std::shared_ptr<thread_worker>> workers_;
    std::vector<std::unordered_set<std::string>> groups_;
    mutable std::shared_mutex mutex_;
};

class message_queue_t final
{
public:
    using message_type = std::any;
    using message_handle = unsigned long long;

private:
    class handler_chain
    {
    public:
        using handler_type = std::function<void(const std::string&, message_type&)>;

        explicit handler_chain(const std::string &topic, std::shared_ptr<thread_pool_t> &threads, std::atomic<message_handle> &idx)
            : topic_{ topic }, threads_{ threads }, idx_{ idx } {
            threads->add_group(topic);
        }
        ~handler_chain() {
            threads_->remove_group(topic_);
        }

        message_handle add_handler(handler_type handler, bool once = false) {
            auto h = std::make_shared<handler_t>();
            h->func_ = handler;
            h->once_ = once;
            h->called_ = false;

            std::unique_lock<std::shared_mutex> lock{ handler_mutex_ };

            auto flag = idx_.fetch_add(1);
            handlers_[flag] = std::move(h);
            return flag;
        }

        void remove_handler(message_handle handle) {
            std::unique_lock<std::shared_mutex> lock{ handler_mutex_ };
            auto it = handlers_.find(handle);
            if (it != handlers_.end()) {
                handlers_.erase(it);
            }
        }

        void publish(const std::any &message, bool sync = false) {
            auto m = std::make_shared<message_t>();
            m->message_ = message;

            if (sync) {
                do_publish(m);
            } else {
                threads_->push_to_group(topic_, &handler_chain::do_publish, this, m);
            }
        }

        bool empty() const {
            std::shared_lock<std::shared_mutex> lock{ handler_mutex_ };
            return handlers_.empty();
        }

    private:
        struct message_t
        {
            message_type message_;
        };

        struct handler_t
        {
            bool once_;
            bool called_;
            handler_type func_;
            std::mutex mutex_;
        };

        void do_publish(const std::shared_ptr<message_t> &message) {
            std::vector<message_handle> rms;
            {
                std::shared_lock<std::shared_mutex> lock{ handler_mutex_ };
                for (auto it = handlers_.begin(); it != handlers_.end(); ++it) {
                    auto &h = it->second;
                    {
                        std::unique_lock<std::mutex> hlock{ h->mutex_ };
                        if (h->once_ && h->called_) {
                            rms.push_back(it->first);
                            continue;
                        }
                        h->called_ = true;
                    }
                    h->func_(topic_, message->message_);
                }
            }
            if (!rms.empty()) {
                std::unique_lock<std::shared_mutex> lock{ handler_mutex_ };
                for (auto it = rms.begin(); it != rms.end(); ++it) {
                    auto h = handlers_.find(*it);
                    if (h != handlers_.end()) {
                        handlers_.erase(h);
                    }
                }
            }
        }

    private:
        std::string topic_;
        std::shared_ptr<thread_pool_t> threads_;
        std::unordered_map<message_handle, std::shared_ptr<handler_t>> handlers_;
        mutable std::shared_mutex handler_mutex_;
        std::atomic<message_handle> &idx_;
    };

public:
    message_queue_t(size_t threads = std::thread::hardware_concurrency())
        : threads_{ new thread_pool_t{ threads } } {}
    ~message_queue_t() = default;

    message_queue_t(message_queue_t&) = delete;
    message_queue_t(message_queue_t&&) = delete;
    message_queue_t& operator=(message_queue_t&) = delete;
    message_queue_t& operator=(message_queue_t&&) = delete;

    void publish(const std::string &topic, const std::any &message, bool sync = false) {
        std::shared_lock<std::shared_mutex> lock{ mutex_ };
        auto it = handlers_.find(topic);
        if (it != handlers_.end()) {
            it->second->publish(message, sync);
        }
    }

    message_handle subscribe(const std::string &topic, handler_chain::handler_type handler, bool once = false) {
        std::unique_lock<std::shared_mutex> lock{ mutex_ };

        message_handle handle;
        auto it = handlers_.find(topic);
        if (it != handlers_.end()) {
            handle = it->second->add_handler(handler, once);
        } else {
            auto h = std::make_shared<handler_chain>(topic, threads_, curr_);
            handle = h->add_handler(handler, once);
            handlers_[topic] = std::move(h);
        }

        return handle;
    }

    void unsubscribe(message_handle handler) {
        std::unique_lock<std::shared_mutex> lock{ mutex_ };
        for (auto it = handlers_.begin(); it != handlers_.end();) {
            it->second->remove_handler(handler);
            if (it->second->empty()) {
                it = handlers_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<thread_pool_t> threads_;
    std::atomic<message_handle> curr_;
    std::unordered_map<std::string, std::shared_ptr<handler_chain>> handlers_;
};
