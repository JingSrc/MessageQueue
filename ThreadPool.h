#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>

class ThreadPool
{
public:
    explicit ThreadPool(size_t);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool(const ThreadPool&&) = delete;

    ThreadPool &operator=(const ThreadPool&) = delete;
    ThreadPool &operator=(const ThreadPool&&) = delete;

    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
#if __cplusplus < 201703L
    -> std::future<typename std::result_of<F(Args...)>::type>;
#else
    -> std::future<typename std::invoke_result<F, Args...>::type>;
#endif

private:
    std::vector<std::thread>          m_workers;
    std::queue<std::function<void()>> m_tasks;
    std::mutex                        m_queue_mutex;
    std::condition_variable           m_condition;
    bool                              m_stop{false};
};

inline ThreadPool::ThreadPool(size_t threads)
{
    for (size_t i = 0; i < threads; ++i)
        m_workers.emplace_back([this]{
            for(;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock{this->m_queue_mutex};
                    this->m_condition.wait(lock, [this]{ return this->m_stop || !this->m_tasks.empty(); });
                    if (this->m_stop && this->m_tasks.empty()) {
                        return;
                    }
                    task = std::move(this->m_tasks.front());
                    this->m_tasks.pop();
                }

                task();
            }
        });
}

template<class F, class... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
#if __cplusplus < 201703L
-> std::future<typename std::result_of<F(Args...)>::type>
#else
-> std::future<typename std::invoke_result<F, Args...>::type>
#endif
{
#if __cplusplus < 201703L
    using return_type = typename std::result_of<F(Args...)>::type;
#else
    using return_type = typename std::invoke_result<F, Args...>::type;
#endif

    auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock{m_queue_mutex};
        if (m_stop) {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }

        m_tasks.emplace([task](){ (*task)(); });
    }

    m_condition.notify_one();
    return res;
}

inline ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock{m_queue_mutex};
        m_stop = true;
    }

    m_condition.notify_all();

    for (std::thread &worker: m_workers) {
        worker.join();
    }
}
