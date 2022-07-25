#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <typeindex>
#include <string>

#include "SharedMutex.h"
#include "ThreadPool.h"

#define MESSAGE_DISABLE_COPY(_ClassName_) \
    _ClassName_(_ClassName_&) = delete; \
    _ClassName_(_ClassName_&&) = delete; \
    _ClassName_& operator=(_ClassName_&) = delete; \
    _ClassName_& operator=(_ClassName_&&) = delete;

template<class T>
class Message final
{
    MESSAGE_DISABLE_COPY(Message)
public:
    using payload_type = T;
    using payload_timestamp = std::chrono::steady_clock::time_point;

    Message(const std::string &topic, payload_type payload)
            : topic_{topic}, payload_{payload}, timestamp_{std::chrono::steady_clock::now()} {}
    ~Message() = default;

    const std::string &topic() const { return topic_; }
    const payload_timestamp &timestamp() const { return timestamp_; }

    payload_type payload() const { return payload_; }

private:
    std::string       topic_;
    payload_type      payload_;
    payload_timestamp timestamp_;
};

template<typename T>
using MessagePointer = std::shared_ptr<Message<T>>;

class MessageHandlerIndex final
{
    MESSAGE_DISABLE_COPY(MessageHandlerIndex)
public:
    using index_type = int;

    static index_type create_index()
    {
        auto &inst = instance();
        return inst.counter_++;
    }

private:
    MessageHandlerIndex() = default;

    static MessageHandlerIndex &instance()
    {
        static MessageHandlerIndex counter;
        return counter;
    }

private:
    std::atomic_int counter_{0};
};

template<typename T>
class IMessageHandler
{
    MESSAGE_DISABLE_COPY(IMessageHandler)
public:
    using value_type      = T;
    using message_pointer = MessagePointer<value_type>;

    explicit IMessageHandler(const std::string &topic, bool once = false)
            : once_{once}, topic_{topic}, index_{MessageHandlerIndex::create_index()} {}
    virtual ~IMessageHandler() = default;

    int index() const { return index_; }
    const std::string &topic() const { return topic_; }
    bool once() const { return once_; }

    virtual void handle(const message_pointer &message) = 0;
    virtual void handle_p(const message_pointer &message)
    {
        std::unique_lock<std::mutex> lock{mutex_};
        if (once_ && called_) {
            return;
        }

        handle(message);
        called_ = true;
    }

private:
    bool                            once_;
    bool                            called_{false};
    MessageHandlerIndex::index_type index_{-1};
    std::string                     topic_;
    std::mutex                      mutex_;
};

template<typename T>
class MessageHandlerDefaultImpl final : public IMessageHandler<T>
{
public:
    using message_pointer = typename IMessageHandler<T>::message_pointer;
    using message_handler = std::function<void(const message_pointer &)>;

    explicit MessageHandlerDefaultImpl(const std::string &topic, message_handler fn, bool once = false)
            : IMessageHandler<T>{topic, once}, handler_{fn} {}
    virtual ~MessageHandlerDefaultImpl() = default;

    virtual void handle(const message_pointer &message) override
    {
        handler_(message);
    }

private:
    message_handler handler_;
};

class IMessageQueueType
{
public:
    IMessageQueueType() = default;
    virtual ~IMessageQueueType() = default;

    virtual std::type_index type() const = 0;
};

template<typename T>
class MessageQueueType final : public IMessageQueueType
{
    MESSAGE_DISABLE_COPY(MessageQueueType)
public:
    using value_type              = T;
    using message_type            = Message<value_type>;
    using message_pointer         = MessagePointer<value_type>;
    using message_handler         = IMessageHandler<value_type>;
    using message_handler_pointer = std::shared_ptr<message_handler>;
    using message_handler_funtion = std::function<void(const message_pointer &)>;

private:
    using message_handlers = std::unordered_map<MessageHandlerIndex::index_type, message_handler_pointer>;

public:
    explicit MessageQueueType(const std::shared_ptr<ThreadPool> &threads)
            : threads_{threads} {}
    virtual ~MessageQueueType() override = default;

    virtual std::type_index type() const override { return std::type_index(typeid(value_type)); }

    bool publish(const std::string &topic, value_type value, bool is_async = true)
    {
        auto ptr = std::make_shared<message_type>(topic, value);
        return publish(ptr, is_async);
    }

    bool publish(const message_pointer &ptr, bool is_async = true)
    {
        if (is_async) {
            return do_publish_async(ptr);
        }
        return do_publish(ptr);
    }

    message_handler_pointer subscribe(message_handler_pointer handler)
    {
        unique_lock<shared_mutex> lock{mutex_};
        auto &h = handlers_[handler->topic()];
        h[handler->index()] = handler;
        return handler;
    }

    message_handler_pointer subscribe(const std::string &topic, message_handler_funtion handler, bool once = false)
    {
        auto ptr = std::make_shared<MessageHandlerDefaultImpl<T>>(topic, handler, once);
        return subscribe(ptr);
    }

    void unsubscribe(message_handler_pointer handler)
    {
        unique_lock<shared_mutex> lock{mutex_};

        auto it = handlers_.find(handler->topic());
        if (it != handlers_.end()) {
            auto &handlers = it->second;
            auto hit = handlers.find(handler->index());
            if (hit != handlers.end()) {
                handlers.erase(hit);
                if (handlers.empty()) {
                    handlers_.erase(it);
                }
            }
        }
    }

    bool empty() const
    {
        shared_lock<shared_mutex> lock{mutex_};
        return handlers_.empty();
    }

private:
    bool do_publish(const std::string &topic, std::function<void(message_handler_pointer)> handler)
    {
        std::vector<message_handler_pointer> rmv;
        {
            shared_lock<shared_mutex> lock{mutex_};

            auto it = handlers_.find(topic);
            if (it != handlers_.end()) {
                auto &handlers = it->second;
                for (auto hit = handlers.begin(); hit != handlers.end(); ++hit) {
                    handler(hit->second);
                    if (hit->second->once()) {
                        rmv.push_back(hit->second);
                    }
                }
            }
        }

        for (auto it = rmv.begin(); it != rmv.end(); ++it) {
            unsubscribe(*it);
        }

        return true;
    }

    bool do_publish(const message_pointer &ptr)
    {
        return do_publish(ptr->topic(), [ptr](message_handler_pointer handler) {
            handler->handle_p(ptr);
        });
    }

    bool do_publish_async(const message_pointer &ptr)
    {
        if (!threads_) {
            return do_publish(ptr);
        }

        return do_publish(ptr->topic(), [this, ptr](message_handler_pointer handler) {
            threads_->enqueue([ptr, handler]{
                handler->handle_p(ptr);
            });
        });
    }

private:
    std::shared_ptr<ThreadPool>                       threads_;
    mutable shared_mutex                              mutex_;
    std::unordered_map<std::string, message_handlers> handlers_;
};

class MessageQueue
{
    MESSAGE_DISABLE_COPY(MessageQueue)
public:
    explicit MessageQueue(size_t threads = std::thread::hardware_concurrency())
            : threads_{new ThreadPool{threads}} {}
    ~MessageQueue() = default;

    template<typename T>
    bool publish(const std::string &topic, T value, bool is_async)
    {
        auto ptr = std::make_shared<typename MessageQueueType<T>::message_type>(topic, value);
        return publish<T>(ptr, is_async);
    }

    template<typename T>
    bool publish(const typename MessageQueueType<T>::message_pointer &ptr, bool is_async = true)
    {
        shared_lock<shared_mutex> lock{ mutex_ };
        auto qu = get_queue<T>();
        if (qu) {
            return qu->publish(ptr, is_async);
        }
        return false;
    }

    template<typename T>
    std::shared_ptr<IMessageHandler<T>> subscribe(typename MessageQueueType<T>::message_handler_pointer handler)
    {
        unique_lock<shared_mutex> lock{mutex_};
        auto qu = get_queue_default<T>();
        return qu->subscribe(handler);
    }

    template<typename T>
    std::shared_ptr<IMessageHandler<T>> subscribe(const std::string &topic, typename MessageQueueType<T>::message_handler_funtion handler, bool once = false)
    {
        unique_lock<shared_mutex> lock{ mutex_ };
        auto qu = get_queue_default<T>();
        return qu->subscribe(topic, handler, once);
    }

    template<typename T>
    void unsubscribe(int index)
    {
        unique_lock<shared_mutex> lock{mutex_};
        auto qu = get_queue<T>();
        if (qu) {
            qu->unsubscribe(index);
            if (qu->empty()) {
                queues_.erase(queues_.find(std::type_index(typeid(T))));
            }
        }
    }

private:
    template<typename T>
    std::shared_ptr<MessageQueueType<T>> get_queue()
    {
        using message_queue_type = MessageQueueType<T>;

        std::shared_ptr<message_queue_type> ptr;

        auto tp = std::type_index(typeid(T));
        auto it = queues_.find(tp);
        if (it != queues_.end()) {
            ptr = std::dynamic_pointer_cast<message_queue_type>(it->second);
        }

        return ptr;
    }

    template<typename T>
    std::shared_ptr<MessageQueueType<T>> get_queue_default()
    {
        auto qu = get_queue<T>();
        if (!qu) {
            qu = std::make_shared<MessageQueueType<T>>(threads_);
            queues_[std::type_index(typeid(T))] = qu;
        }

        return qu;
    }

private:
    std::shared_ptr<ThreadPool> threads_;
    shared_mutex mutex_;
    std::unordered_map<std::type_index, std::shared_ptr<IMessageQueueType>> queues_;
};

template<>
bool MessageQueue::publish<char *>(const typename MessageQueueType<char *>::message_pointer &ptr, bool is_async)
{
    shared_lock<shared_mutex> lock{ mutex_ };
    auto qu = get_queue<char *>();
    if (qu) {
        return qu->publish(ptr, is_async);
    }

    auto cc_qu = get_queue<const char *>();
    if (cc_qu) {
        auto cc_ptr = std::make_shared<Message<const char *>>(ptr->topic(), ptr->payload());
        return cc_qu->publish(cc_ptr, is_async);
    }

    auto str_qu = get_queue<std::string>();
    if (str_qu) {
        auto str_ptr = std::make_shared<Message<std::string>>(ptr->topic(), ptr->payload());
        return str_qu->publish(str_ptr, is_async);
    }

    return false;
}

template<>
bool MessageQueue::publish<const char *>(const typename MessageQueueType<const char *>::message_pointer &ptr, bool is_async)
{
    shared_lock<shared_mutex> lock{ mutex_ };

    auto cc_qu = get_queue<const char *>();
    if (cc_qu) {
        return cc_qu->publish(ptr, is_async);
    }

    auto str_qu = get_queue<std::string>();
    if (str_qu) {
        auto str_ptr = std::make_shared<Message<std::string>>(ptr->topic(), ptr->payload());
        return str_qu->publish(str_ptr, is_async);
    }

    return false;
}
