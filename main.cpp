#include "MessageQueue.h"

int main() {
    message_queue_t mq;
    mq.subscribe("111", [](const std::string &topic, const message_queue_t::message_type &message){
        static int i = 0;
        std::cout << i++ << " " <<  message.value<std::string>() << std::endl;
    });
    mq.subscribe("222", [](const std::string &topic, const message_queue_t::message_type &message){
        static int i = 0;
        std::cout << i++ << " " <<  message.value<std::string>() << std::endl;
    });
    mq.subscribe("222", [](const std::string &topic, const message_queue_t::message_type &message){
        static int i = 0;
        std::cout << i++ << " " <<  message.value<std::string>() << std::endl;
    }, true);

    for (int i = 0; i < 10; ++i) {
        mq.publish("111", "111111");
        mq.publish("222", "222222");
    }

    char c;
    std::cin >> c;
    std::cout << "=============================" << std::endl;

    return 0;
}
