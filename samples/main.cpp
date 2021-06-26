#include <iostream>
#include <thread>
#include <chrono>
#include <sstream>
#include <cstring>
#include <functional>

#include <ipmq.h>
#include "cpp/ipmq.h"

//namespace ipmq {
//	class Commands {
//	private:
//		IPMQCommands* mCommands;
//	public:
//		explicit Commands(IPMQCommands* commands)
//			: mCommands(commands) {
//
//		}
//
//		void acknowledgment(std::uint64_t queueId, std::uint64_t messageId) {
//			ipmq_consumer_add_ack_command(mCommands, queueId, messageId);
//		}
//
//		void negativeAcknowledgment(std::uint64_t queueId, std::uint64_t messageId) {
//			ipmq_consumer_add_nack_command(mCommands, queueId, messageId);
//		}
//
//		void stopConsume(std::uint64_t queueId) {
//			ipmq_consumer_add_stop_consume_command(mCommands, queueId);
//		}
//	};
//
//	class Consumer {
//	private:
//		IPMQConsumer* mConsumer;
//	public:
//		explicit Consumer(const std::string& path) {
//			mConsumer = ipmq_consumer_create(path.c_str());
//			if (mConsumer == nullptr) {
//				throw std::runtime_error("Failed to connect to producer.");
//			}
//		}
//
//		~Consumer() {
//			if (mConsumer != nullptr) {
//				ipmq_consumer_destroy(mConsumer);
//			}
//		}
//
//		Consumer(const Consumer&) = delete;
//		Consumer& operator=(const Consumer&) = delete;
//
//		void createQueue(const std::string& name, bool autoDelete = true, double ttl = -1.0) {
//			if (ipmq_consumer_create_queue(mConsumer, name.c_str(), autoDelete, ttl) != 0) {
//				throw std::runtime_error("Failed to create queue.");
//			}
//		}
//
//		void bindQueue(const std::string& name, const std::string& pattern) {
//			if (ipmq_consumer_bind_queue(mConsumer, name.c_str(), pattern.c_str()) != 0) {
//				throw std::runtime_error("Failed to bind to queue.");
//			}
//		}
//
//		using Callback = std::function<void (Commands&, uint64_t, const char*, uint64_t, const uint8_t*, uintptr_t)>;
//		void startConsumeQueue(const std::string& name, Callback callback) {
//			auto result = ipmq_consumer_start_consume_queue(
//				mConsumer,
//				name.c_str(),
//				[](IPMQCommands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size, void* userPtr) {
//					auto callback = (Callback*)userPtr;
//					Commands commandsWrapper(commands);
//					(*callback)(commandsWrapper, queueId, routingKey, messageId, message, size);
//				},
//				&callback
//			);
//
//			if (result != 0) {
//				throw std::runtime_error("Failed to consume queue.");
//			}
//		}
//	};
//
//	class Producer {
//	private:
//		IPMQProducer* mProducer;
//	public:
//		explicit Producer(const std::string& path, const std::string& sharedMemoryFile, std::size_t sharedMemorySize) {
//			mProducer = ipmq_producer_create(path.c_str(), sharedMemoryFile.c_str(), sharedMemorySize);
//			if (mProducer == nullptr) {
//				throw std::runtime_error("Failed to create producer.");
//			}
//		}
//
//		~Producer() {
//			if (mProducer != nullptr) {
//				ipmq_producer_destroy(mProducer);
//			}
//		}
//
//		Producer(const Producer&) = delete;
//		Producer& operator=(const Producer&) = delete;
//
//		void publishBytes(const std::string& routingKey, std::uint8_t* message, std::size_t messageSize) {
//			if (ipmq_producer_publish_bytes(mProducer, routingKey.c_str(), message, messageSize) != 0) {
//				throw std::runtime_error("Failed to publish message.");
//			}
//		}
//
//		IPMQProducer* underlying() {
//			return mProducer;
//		}
//	};
//
//	class MemoryAllocation {
//	private:
//		IPMQMemoryAllocation* mAllocation;
//	public:
//		explicit MemoryAllocation(Producer& producer, std::size_t size) {
//			mAllocation = ipmq_producer_allocate(producer.underlying(), size);
//			if (mAllocation == nullptr) {
//				throw std::runtime_error("Failed to allocate.");
//			}
//		}
//
//		~MemoryAllocation() {
//			if (mAllocation != nullptr) {
//				ipmq_producer_return_allocation(mAllocation);
//			}
//		}
//
//		MemoryAllocation(const MemoryAllocation&) = delete;
//		MemoryAllocation& operator=(const MemoryAllocation&) = delete;
//
//		std::uint8_t* data() {
//			return ipmq_producer_allocation_get_ptr(mAllocation);
//		}
//	};
//}

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
				[](IPMQCommands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size, void* userPt) {
					std::string messageStr((const char*)message, size);
					std::cout << "(" << messageId << ", " << routingKey << "): " << messageStr << std::endl;
					ipmq_consumer_add_ack_command(commands, queueId, messageId);

					if (messageId == 10) {
						ipmq_consumer_add_stop_consume_command(commands, queueId);
					}
				},
				nullptr
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
				std::memcpy(allocation_ptr, (std::uint8_t*)message.data(), message.size());
				ipmq_producer_publish(producer, "test", allocation);
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
