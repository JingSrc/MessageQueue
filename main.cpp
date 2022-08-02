#include <iostream>

#include "MessageQueue.h"

struct MyEvent
{
    MyEvent() = default;
    MyEvent(int i, const std::string &a) : i{i}, a{a} {}
    virtual ~MyEvent() = default;

    virtual void write(std::ostream &o)
    {
        o << "MyEvent{ i = " << i << ", a = " << a << " }";
    }

	int i{111111};
	std::string a{"xxxxxxxxxxxxx"};
};

struct MyDerivedEvent : public MyEvent
{
    MyDerivedEvent() = default;
    MyDerivedEvent(int i, int j, const std::string &a, const std::string &b) : MyEvent{i, a}, j{j}, b{b} {}
    virtual ~MyDerivedEvent() override = default;

    virtual void write(std::ostream &o)
    {
        MyEvent::write(o);
        o << "MyDerivedEvent{ j = " << j << ", a = " << b << " }";
    }

    int j{222222};
    std::string b{"yyyyyyyyyyyyy"};
};

class MyMessage : public IMessageHandler<std::shared_ptr<MyEvent>>
{
public:
	explicit MyMessage(bool once = false)
		: IMessageHandler<std::shared_ptr<MyEvent>>{ "abc", once } {}

    virtual void handle(const message_pointer &message) override
	{
		auto ev = message->payload();
		std::cout << "my event: " << ev->i << ev->a << std::endl;

        auto de = std::dynamic_pointer_cast<MyDerivedEvent>(ev);
        if (de) {
            std::cout << "my derived event: " << de->j << de->b << std::endl;
        }
	}
};

int main() {
    using IntQueue = MessageQueue<int>;
    IntQueue iq;

    auto i1 = iq.subscribe("1", [](const IntQueue::message_pointer &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i2 = iq.subscribe("2", [](const IntQueue::message_pointer &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i3 = iq.subscribe("3", [](const IntQueue::message_pointer &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    });
    auto i4 = iq.subscribe("3", [](const IntQueue::message_pointer &msg){
        std::cout << msg->topic() << msg->payload() << std::endl;
    }, true);

    iq.publish("3", 5, false);
    iq.publish("3", 10, true);

    iq.unsubscribe(i1);

    using EventQueue = MessagePointerQueue<MyEvent>;
    EventQueue eq;

    auto i5 = eq.subscribe("1", [](const EventQueue::message_pointer &msg){
        std::cout << msg->topic() << " ";
        msg->payload()->write(std::cout);
        std::cout << std::endl;
    });

    eq.publish("1", std::make_shared<MyEvent>(1, "aaa"), true);
    eq.publish("1", std::make_shared<MyDerivedEvent>(1, 2, "aaa", "bbb"), true);

    std::this_thread::sleep_for(std::chrono::seconds(2));
	std::cout << "=============================" << std::endl;

    return 0;
}
