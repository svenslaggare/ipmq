#include <iostream>
#include <thread>
#include <chrono>
#include <sstream>
#include <cstring>

#include <ipmq.h>
#include "cpp/ipmq.hpp"

int main(int argc, const char* argv[]) {
	std::string command = "consumer";
	if (argc > 1) {
		command = argv[1];
	}
	ipmq::enableLogging();

	if (command == "consumer") {
		auto consumer = ipmq_consumer_create("../test.queue", nullptr, 0);

		if (consumer != nullptr) {
			ipmq_consumer_create_queue(consumer, "test", true, -1.0, nullptr, 0);
			ipmq_consumer_bind_queue(consumer, "test", ".*", nullptr, 0);

			ipmq_consumer_start_consume_queue(
				consumer,
				"test",
				[](IPMQCommands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size, void* userPt) {
					std::string messageStr((const char*)message, size);
					std::cout << "(" << messageId << ", " << routingKey << "): " << messageStr << std::endl;
					ipmq_consumer_add_ack_command(commands, queueId, messageId);

					if (messageId == 10) {
						ipmq_consumer_add_stop_consume_command(commands, queueId);
					}
				},
				nullptr,
				nullptr, 0
			);

			ipmq_consumer_destroy(consumer);
		} else {
			std::cout << "Failed to create consumer" << std::endl;
		}
	}

	if (command == "consumer_cpp") {
		ipmq::Consumer consumer("../test.queue");
		consumer.createQueue("test");
		consumer.bindQueue("test", ".*");

		consumer.startConsumeQueue(
			"test",
			[&](ipmq::Commands& commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size) {
				std::string messageStr((const char*)message, size);
				std::cout << "(" << messageId << ", " << routingKey << ", " << &consumer << "): " << messageStr << std::endl;
				commands.acknowledgment(queueId, messageId);

				if (messageId == 10) {
					commands.stopConsume(queueId);
				}
			}
		);
	}

	if (command == "producer") {
		auto producer = ipmq_producer_create("../test.queue", "/dev/shm/test.data", 2048, nullptr, 0);
		if (producer != nullptr) {
			int number = 1;
			while (true) {
				std::stringstream messageStream;
				messageStream << "Hello, World #" << number << "!";
				std::string message = messageStream.str();

//				ipmq_producer_publish_bytes(producer, "test", (unsigned char*)message.data(), message.size(), nullptr, 0);

				auto allocation = ipmq_producer_allocate(producer, message.size(), nullptr, 0);
				auto allocation_ptr = ipmq_producer_allocation_get_ptr(allocation);
				std::memcpy(allocation_ptr, (std::uint8_t*)message.data(), message.size());
				ipmq_producer_publish(producer, "test", allocation, nullptr, 0);
				ipmq_producer_return_allocation(allocation);

				std::this_thread::sleep_for(std::chrono::milliseconds(200));
				number += 1;
			}

			ipmq_producer_destroy(producer);
		} else {
			std::cout << "Failed to create producer" << std::endl;
		}
	}

	if (command == "producer_cpp") {
		ipmq::Producer producer("../test.queue", "/dev/shm/test.data", 2048);

		int number = 1;
		while (true) {
			std::stringstream messageStream;
			messageStream << "Hello, World #" << number << "!";
			std::string message = messageStream.str();

//			producer.publishBytes("test", (std::uint8_t*)message.data(), message.size());

			ipmq::MemoryAllocation allocation(producer, message.size());
			std::memcpy(allocation.data(), (std::uint8_t*)message.data(), message.size());
			producer.publish("test", std::move(allocation));

			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			number += 1;
		}
	}

	return 0;
}
