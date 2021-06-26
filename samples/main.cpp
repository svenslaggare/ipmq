#include <iostream>
#include <thread>
#include <chrono>
#include <sstream>
#include <cstring>

#include <ipmq.h>

int main(int argc, const char* argv[]) {
	std::string command = "consumer";
	if (argc > 1) {
		command = argv[1];
	}

	if (command == "consumer") {
		auto consumer = ipmq_consumer_create("../test.queue");

		if (consumer != nullptr) {
			ipmq_consumer_create_queue(consumer, "test", true, -1.0);
			ipmq_consumer_bind_queue(consumer, "test", ".*");

			ipmq_consumer_start_consume_queue(
				consumer,
				"test",
				[](Commands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size) {
					std::string messageStr((const char*)message, size);
					std::cout << "(" << messageId << ", " << routingKey << "): " << messageStr << std::endl;
					ipmq_consumer_add_ack_command(commands, queueId, messageId);

					if (messageId == 10) {
						ipmq_consumer_add_stop_consume_command(commands, queueId);
					}
				}
			);

			ipmq_consumer_destroy(consumer);
		}
	}

	if (command == "producer") {
		auto producer = ipmq_producer_create("../test.queue", "/dev/shm/test.data", 2048);
		if (producer != nullptr) {
			int number = 1;
			while (true) {
				std::stringstream messageStream;
				messageStream << "Hello, World #" << number << "!";
				std::string message = messageStream.str();

//				ipmq_producer_publish_bytes(producer, "test", (unsigned char*)message.data(), message.size());

				auto allocation = ipmq_producer_allocate(producer, message.size());
				auto allocation_ptr = ipmq_producer_allocation_get_ptr(allocation);
				std::memcpy(allocation_ptr, (unsigned char*)message.data(), message.size());
				ipmq_producer_publish(producer, "test", allocation);
				ipmq_producer_return_allocation(allocation);

				std::this_thread::sleep_for(std::chrono::milliseconds(200));
				number += 1;
			}

			ipmq_producer_destroy(producer);
		}
	}

	return 0;
}
