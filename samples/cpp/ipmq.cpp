#include "ipmq.h"

#include <iostream>

namespace ipmq {
	Commands::Commands(IPMQCommands* commands)
		: mCommands(commands) {

	}

	void Commands::acknowledgment(std::uint64_t queueId, std::uint64_t messageId) {
		ipmq_consumer_add_ack_command(mCommands, queueId, messageId);
	}

	void Commands::negativeAcknowledgment(std::uint64_t queueId, std::uint64_t messageId) {
		ipmq_consumer_add_nack_command(mCommands, queueId, messageId);
	}

	void Commands::stopConsume(std::uint64_t queueId) {
		ipmq_consumer_add_stop_consume_command(mCommands, queueId);
	}

	Consumer::Consumer(const std::string& path) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		mConsumer = ipmq_consumer_create(path.c_str(), &errorMsg[0], errorMsgLength);
		if (mConsumer == nullptr) {
			throw std::runtime_error("Failed to connect to producer: " + std::string(errorMsg) + ".");
		}
	}

	Consumer::~Consumer() {
		if (mConsumer != nullptr) {
			ipmq_consumer_destroy(mConsumer);
		}
	}

	void Consumer::createQueue(const std::string& name, bool autoDelete, double ttl) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		if (ipmq_consumer_create_queue(mConsumer, name.c_str(), autoDelete, ttl, &errorMsg[0], errorMsgLength) != 0) {
			throw std::runtime_error("Failed to create queue: " + std::string(errorMsg) + ".");
		}
	}

	void Consumer::bindQueue(const std::string& name, const std::string& pattern) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		if (ipmq_consumer_bind_queue(mConsumer, name.c_str(), pattern.c_str(), &errorMsg[0], errorMsgLength) != 0) {
			throw std::runtime_error("Failed to bind to queue: " + std::string(errorMsg) + ".");
		}
	}

	void Consumer::startConsumeQueue(const std::string& name, Callback callback) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		auto result = ipmq_consumer_start_consume_queue(
			mConsumer,
			name.c_str(),
			[](IPMQCommands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size, void* userPtr) {
				auto callback = (Callback*)userPtr;
				Commands commandsWrapper(commands);
				(*callback)(commandsWrapper, queueId, routingKey, messageId, message, size);
			},
			&callback,
			&errorMsg[0], errorMsgLength
		);

		if (result != 0) {
			throw std::runtime_error("Failed to consume queue: " + std::string(errorMsg) + ".");
		}
	}

	Producer::Producer(const std::string& path, const std::string& sharedMemoryFile, std::size_t sharedMemorySize) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		mProducer = ipmq_producer_create(path.c_str(), sharedMemoryFile.c_str(), sharedMemorySize, &errorMsg[0], errorMsgLength);
		if (mProducer == nullptr) {
			throw std::runtime_error("Failed to create producer: " + std::string(errorMsg) + ".");
		}
	}

	Producer::~Producer() {
		if (mProducer != nullptr) {
			ipmq_producer_destroy(mProducer);
		}
	}

	void Producer::publishBytes(const std::string& routingKey, std::uint8_t* message, std::size_t messageSize) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		if (ipmq_producer_publish_bytes(mProducer, routingKey.c_str(), message, messageSize, errorMsg, errorMsgLength) != 0) {
			throw std::runtime_error("Failed to publish message: " + std::string(errorMsg) + ".");
		}
	}

	void Producer::publish(const std::string& routingKey, MemoryAllocation allocation) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		if (ipmq_producer_publish(mProducer, routingKey.c_str(), allocation.underlying(), errorMsg, errorMsgLength) != 0) {
			throw std::runtime_error("Failed to publish message: " + std::string(errorMsg) + ".");
		}
	}

	IPMQProducer* Producer::underlying() {
		return mProducer;
	}

	MemoryAllocation::MemoryAllocation(Producer& producer, std::size_t size) {
		constexpr std::size_t errorMsgLength = 1024;
		char errorMsg[errorMsgLength];

		mAllocation = ipmq_producer_allocate(producer.underlying(), size, errorMsg, errorMsgLength);
		if (mAllocation == nullptr) {
			throw std::runtime_error("Failed to allocate: " + std::string(errorMsg) + ".");
		}
	}

	MemoryAllocation::~MemoryAllocation() {
		if (mAllocation != nullptr) {
			ipmq_producer_return_allocation(mAllocation);
		}
	}

	MemoryAllocation::MemoryAllocation(MemoryAllocation&& other) noexcept
		: mAllocation(other.mAllocation) {
		other.mAllocation = nullptr;
	}

	MemoryAllocation& MemoryAllocation::operator=(MemoryAllocation&& rhs) noexcept {
		if (this != &rhs) {
			mAllocation = rhs.mAllocation;
			rhs.mAllocation = nullptr;
		}

		return *this;
	}

	std::uint8_t* MemoryAllocation::data() {
		return ipmq_producer_allocation_get_ptr(mAllocation);
	}

	IPMQMemoryAllocation* MemoryAllocation::underlying() {
		return mAllocation;
	}
}