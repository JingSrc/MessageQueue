#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>
#include <set>

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

template<typename T>
class IMessageHandler
{
    MESSAGE_DISABLE_COPY(IMessageHandler)
public:
    using value_type      = T;
    using message_pointer = MessagePointer<value_type>;

    explicit IMessageHandler(const std::string &topic, bool once = false)
            : once_{once}, topic_{topic} {}
    virtual ~IMessageHandler() = default;

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
    bool        once_;
    bool        called_{false};
    std::string topic_;
    std::mutex  mutex_;
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

template<typename T>
class MessageQueue final
{
    MESSAGE_DISABLE_COPY(MessageQueue)
public:
    using value_type              = T;
    using message_type            = Message<value_type>;
    using message_pointer         = MessagePointer<value_type>;
    using message_handler         = IMessageHandler<value_type>;
    using message_handler_pointer = std::shared_ptr<message_handler>;
    using message_handler_funtion = std::function<void(const message_pointer &)>;

private:
    using message_handlers = std::set<message_handler_pointer>;

public:
    explicit MessageQueue()
            : threads_{new ThreadPool{std::thread::hardware_concurrency()}} {}
    ~MessageQueue() = default;

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

        auto &handlers = handlers_[handler->topic()];
        if (handlers.find(handler) == handlers.end()) {
            handlers.insert(handler);
        }

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
            auto hit = handlers.find(handler);
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
                    handler(*hit);
                    if ((*hit)->once()) {
                        rmv.push_back(*hit);
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

