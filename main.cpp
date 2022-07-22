#include <iostream>

#include "MessageQueue.h"

struct MyEvent
{
public:
	int i{1213131};
	std::string b{"xxxxxxxxxxxxx"};
};

class MyMessage : public IMessageHandler<std::shared_ptr<MyEvent>>
{
public:
	explicit MyMessage(bool once = false)
		: IMessageHandler<std::shared_ptr<MyEvent>>{ "abc", once } {}

	virtual void const handle(const message_pointer &message) override
	{
		auto ev = message->payload();
		std::cout << "my event: " << ev->i << ev->b << std::endl;
	}
};

int main() {
    auto threads = std::make_shared<ThreadPool>(5);
    MessageQueueType<int> mt{threads};

    auto i1 = mt.subscribe("1", [](const std::shared_ptr<Message<int>> &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i2 = mt.subscribe("2", [](const std::shared_ptr<Message<int>> &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i3 = mt.subscribe("3", [](const std::shared_ptr<Message<int>> &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i4 = mt.subscribe("3", [](const std::shared_ptr<Message<int>> &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    }, true);

    mt.publish("3", 5, false);
    mt.publish("3", 10, true);

    std::this_thread::sleep_for(std::chrono::seconds(2));
	std::cout << "=============================" << std::endl;

	MessageQueue qu;

	auto i5 = qu.subscribe<int>("1", [](const std::shared_ptr<Message<int>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	}, true);

	auto i6 = qu.subscribe<int>("2", [](const std::shared_ptr<Message<int>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i7 = qu.subscribe<int>("3", [](const std::shared_ptr<Message<int>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i8 = qu.subscribe<int>("3", [](const std::shared_ptr<Message<int>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i9 = qu.subscribe<std::string>("3", [](const std::shared_ptr<Message<std::string>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i10 = qu.subscribe<std::string>("3", [](const std::shared_ptr<Message<std::string>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i12 = qu.subscribe<double>("3", [](const std::shared_ptr<Message<double>> &msg) {
		std::cout << msg->topic() << msg->payload() << std::endl;
	});

	auto i13 = qu.subscribe<std::shared_ptr<MyEvent>>(std::make_shared<MyMessage>());

	qu.publish("3", 5, false);
	qu.publish("3", 10, true);
	qu.publish("3", 10.12, true);
	qu.publish("3", "abcde", true);
	qu.publish("abc", std::make_shared<MyEvent>(), true);

	std::this_thread::sleep_for(std::chrono::seconds(10));

    return 0;
}
