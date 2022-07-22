#pragma once

#include <memory>
#include <unordered_map>
#include <typeindex>
#include <string>

#include "SharedMutex.h"
#include "ThreadPool.h"

template<class T>
class Message
{
public:
    using payload_type = T;
    using payload_timestamp = std::chrono::steady_clock::time_point;

    Message(const std::string &topic, payload_type payload)
            : topic_{topic}, payload_{payload}, timestamp_{std::chrono::steady_clock::now()} {}

    const std::string &topic() const { return topic_; }
    const payload_timestamp &timestamp() const { return timestamp_; }

    payload_type payload() const { return payload_; }

private:
    std::string topic_;
    payload_type payload_;
    payload_timestamp timestamp_;
};

class MessageHandlerCounter
{
public:
    operator int()
    {
        return instance().counter_;
    }

    MessageHandlerCounter &operator++(int)
    {
        instance().counter_++;
        return *this;
    }

    static MessageHandlerCounter &instance()
    {
        static MessageHandlerCounter counter;
        return counter;
    }

private:
    std::atomic_int counter_{0};
};

template<typename T>
class IMessageHandler
{
public:
    using value_type      = T;
    using message_type    = Message<value_type>;
    using message_pointer = std::shared_ptr<message_type>;

    explicit IMessageHandler(const std::string &topic, bool once = false)
        : once_{once}, topic_{topic} {}
    virtual ~IMessageHandler() = default;

    const std::string &topic() const { return topic_; }
    bool once() const { return once_; }

    int index() const { return index_; }
    void set_index(int index)
    {
        if (index_ == -1) {
            index_ = index;
        }
    }

    virtual void const handle(const message_pointer &message) = 0;

private:
    bool once_;
    int index_{-1};
    std::string topic_;
};

template<typename T>
class MessageHandlerDefaultImpl : public IMessageHandler<T>
{
public:
    using message_pointer = typename IMessageHandler<T>::message_pointer;
    using message_handler = std::function<void(const message_pointer &)>;

    explicit MessageHandlerDefaultImpl(const std::string &topic, message_handler fn, bool once = false)
            : IMessageHandler<T>{topic, once}, handler_{fn} {}
    virtual ~MessageHandlerDefaultImpl() = default;

    virtual void const handle(const message_pointer &message) override
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
class MessageQueueType : public IMessageQueueType
{
public:
    using value_type          = T;
    using message_type        = Message<value_type>;
    using message_pointer     = std::shared_ptr<message_type>;
    using message_handler     = IMessageHandler<value_type>;
    using message_handler_ptr = std::shared_ptr<message_handler>;
    using message_handler_fn  = std::function<void(const message_pointer &)>;

private:
    using message_handlers = std::unordered_map<int, message_handler_ptr>;

public:
    explicit MessageQueueType(const std::shared_ptr<ThreadPool> &threads)
        : threads_{threads} {}

    ~MessageQueueType() = default;

    MessageQueueType(MessageQueueType&) = delete;
    MessageQueueType(MessageQueueType&&) = delete;
    MessageQueueType& operator=(MessageQueueType&) = delete;
    MessageQueueType& operator=(MessageQueueType&&) = delete;

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

    int subscribe(message_handler_ptr handler)
    {
        unique_lock<shared_mutex> lock{mutex_};
        auto &subscribers = subscribers_[handler->topic()];
        handler->set_index(MessageHandlerCounter::instance()++);
        subscribers[handler->index()] = handler;
        return handler->index();
    }

    int subscribe(const std::string &topic, message_handler_fn handler, bool once = false)
    {
        auto ptr = std::make_shared<MessageHandlerDefaultImpl<T>>(topic, handler, once);
        return subscribe(ptr);
    }

    void unsubscribe(int index)
    {
        std::string topic;

        unique_lock<shared_mutex> lock{mutex_};
        for (auto it = subscribers_.begin(); it != subscribers_.end(); ++it) {
            auto handler = it->second.find(index);
            if (handler != it->second.end()) {
                topic = handler->second->topic();
                it->second.erase(handler);
                break;
            }
        }

        if (!topic.empty()) {
            auto it = subscribers_.find(topic);
            if (it->second.empty()) {
                subscribers_.erase(it);
            }
        }
    }

	bool empty() const
	{
		shared_lock<shared_mutex> lock{ mutex_ };
		return subscribers_.empty();
	}

private:
    bool do_publish(const std::string &topic, std::function<void(message_handler_ptr)> handler)
    {
        shared_lock<shared_mutex> lock{mutex_};
        auto subscribers = subscribers_.find(topic);
        if (subscribers != subscribers_.end()) {
            for (auto it = subscribers->second.begin(); it != subscribers->second.end();) {
                handler(it->second);
                if (it->second->once())
                    it = subscribers->second.erase(it);
                else
                    ++it;
            }
            if (subscribers->second.empty()) {
                subscribers_.erase(subscribers);
            }
        }
        return true;
    }

    bool do_publish(const message_pointer &ptr)
    {
        return do_publish(ptr->topic(), [ptr](message_handler_ptr handler) {
            handler->handle(ptr);
        });
    }

    bool do_publish_async(const message_pointer &ptr)
    {
        if (!threads_) {
            return do_publish(ptr);
        }

        return do_publish(ptr->topic(), [this, ptr](message_handler_ptr handler) {
            threads_->enqueue([ptr, handler]{
                handler->handle(ptr);
            });
        });
    }

private:
    std::shared_ptr<ThreadPool> threads_;
    mutable shared_mutex mutex_;
    std::unordered_map<std::string, message_handlers> subscribers_;
};

class MessageQueue
{
public:
	explicit MessageQueue(size_t threads = std::thread::hardware_concurrency())
		: threads_{new ThreadPool{threads}} {}

    MessageQueue(MessageQueue&) = delete;
    MessageQueue(MessageQueue&&) = delete;
    MessageQueue& operator=(MessageQueue&) = delete;
    MessageQueue& operator=(MessageQueue&&) = delete;

    template<typename T>
    bool publish(const std::string &topic, T value, bool is_async)
    {
		shared_lock<shared_mutex> lock{ mutex_ };
		auto qu = queue_type<T>::get(this);
		if (qu) {
			return qu->publish(topic, value, is_async);
		}
		return false;
    }

    template<typename T>
    bool publish(const typename MessageQueueType<T>::message_pointer &ptr, bool is_async = true)
    {
		shared_lock<shared_mutex> lock{ mutex_ };
		auto qu = queue_type<T>::get(this);
		if (qu) {
			return qu->publish(ptr, is_async);
		}
		return false;
    }

	template<typename T>
    int subscribe(typename MessageQueueType<T>::message_handler_ptr handler)
    {
        unique_lock<shared_mutex> lock{mutex_};
		auto qu = queue_type<T>::get_default(this);
		return qu->subscribe(handler);
    }

	template<typename T>
    int subscribe(const std::string &topic, typename MessageQueueType<T>::message_handler_fn handler, bool once = false)
    {
		unique_lock<shared_mutex> lock{ mutex_ };
		auto qu = queue_type<T>::get_default(this);
		return qu->subscribe(topic, handler, once);
    }

	template<typename T>
    void unsubscribe(int index)
    {
        unique_lock<shared_mutex> lock{mutex_};
		auto qu = queue_type<T>::get(this);
		if (qu) {
			qu->unsubscribe(index);
			if (qu->empty()) {
				queues_.erase(queues_.find(std::type_index(typeid(T))));
			}
		}
    }

private:
	template<typename T>
	struct queue_type
	{
		using value_type = T;
		using message_queuq_type = MessageQueueType<value_type>;

		static std::shared_ptr<message_queuq_type> get(MessageQueue *mq)
		{
			std::shared_ptr<message_queuq_type> ptr;

			auto tp = std::type_index(typeid(T));
			auto it = mq->queues_.find(tp);
			if (it != mq->queues_.end()) {
				ptr = std::dynamic_pointer_cast<message_queuq_type>(it->second);
			}

			return ptr;
		}

		static std::shared_ptr<message_queuq_type> get_default(MessageQueue *mq)
		{
			auto qu = queue_type<T>::get(mq);
			if (!qu) {
				qu = std::make_shared<message_queuq_type>(mq->threads_);
				mq->queues_[std::type_index(typeid(T))] = qu;
			}

			return qu;
		}
	};

	template<>
	struct queue_type<char *>
	{
		using value_type = std::string;
		using message_queuq_type = MessageQueueType<value_type>;

		static std::shared_ptr<message_queuq_type> get(MessageQueue *mq)
		{
			return queue_type<value_type>::get(mq);
		}

		static std::shared_ptr<message_queuq_type> get_default(MessageQueue *mq)
		{
			return queue_type<value_type>::get_default(mq);
		}
	};

	template<>
	struct queue_type<const char *>
	{
		using value_type = std::string;
		using message_queuq_type = MessageQueueType<value_type>;

		static std::shared_ptr<message_queuq_type> get(MessageQueue *mq)
		{
			return queue_type<value_type>::get(mq);
		}

		static std::shared_ptr<message_queuq_type> get_default(MessageQueue *mq)
		{
			return queue_type<value_type>::get_default(mq);
		}
	};

private:
    std::shared_ptr<ThreadPool> threads_;
    shared_mutex mutex_;
    std::unordered_map<std::type_index, std::shared_ptr<IMessageQueueType>> queues_;
};
