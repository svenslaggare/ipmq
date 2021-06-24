#include <iostream>
#include <thread>
#include <chrono>
#include <sstream>

extern "C" {
	/** Consumer */
	struct Consumer;

	// Create/destroy
	Consumer* ipmq_create_consumer(const char* path);
	void ipmq_destroy_consumer(Consumer* consumer);

	// Queue management
	void ipmq_consumer_create_queue(Consumer* consumer, const char* name, bool auto_delete, double ttl);
	void ipmq_consumer_bind_queue(Consumer* consumer, const char* name, const char* pattern);

	// Consume
	void ipmq_consumer_start_consume_queue(Consumer* consumer, const char* name, void (*callback)(std::uint64_t, const char*, std::uint64_t, const unsigned char*, std::size_t));

	/** Producer */
	struct Producer;

	// Create/destroy
	Producer* ipmq_create_producer(const char* path, const char* shared_memory_path, std::size_t shared_memory_size);
	void ipmq_destroy_producer(Producer* producer);

	// Publishing
	void ipmq_producer_publish_bytes(Producer* producer, const char* routing_key, const unsigned char* message_data, std::size_t message_size);
}

int main() {
	auto consumer = ipmq_create_consumer("../test.queue");

	if (consumer != nullptr) {
		ipmq_consumer_create_queue(consumer, "test", true, -1.0);
		ipmq_consumer_bind_queue(consumer, "test", ".*");

		ipmq_consumer_start_consume_queue(
			consumer,
			"test",
			[](std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size) {
				std::string messageStr((const char*)message, size);
				std::cout << "(" << messageId << ", " << routingKey << "): " << messageStr << std::endl;
			}
		);

		ipmq_destroy_consumer(consumer);
	}

//	auto producer = ipmq_create_producer("../test.queue", "/dev/shm/test.data", 2048);
//	if (producer != nullptr) {
//		int number = 1;
//		while (true) {
//			std::stringstream messageStream;
//			messageStream << "Hello, World #" << number << "!";
//			std::string message = messageStream.str();
//			ipmq_producer_publish_bytes(producer, "test", (unsigned char*)message.data(), message.size());
//
//			std::this_thread::sleep_for(std::chrono::milliseconds(200));
//			number += 1;
//		}
//
//		ipmq_destroy_producer(producer);
//	}

	return 0;
}
