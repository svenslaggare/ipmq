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
		mConsumer = ipmq_consumer_create(path.c_str());
		if (mConsumer == nullptr) {
			throw std::runtime_error("Failed to connect to producer.");
		}
	}

	Consumer::~Consumer() {
		if (mConsumer != nullptr) {
			ipmq_consumer_destroy(mConsumer);
		}
	}

	void Consumer::createQueue(const std::string& name, bool autoDelete, double ttl) {
		if (ipmq_consumer_create_queue(mConsumer, name.c_str(), autoDelete, ttl) != 0) {
			throw std::runtime_error("Failed to create queue.");
		}
	}

	void Consumer::bindQueue(const std::string& name, const std::string& pattern) {
		if (ipmq_consumer_bind_queue(mConsumer, name.c_str(), pattern.c_str()) != 0) {
			throw std::runtime_error("Failed to bind to queue.");
		}
	}

	void Consumer::startConsumeQueue(const std::string& name, Callback callback) {
		auto result = ipmq_consumer_start_consume_queue(
			mConsumer,
			name.c_str(),
			[](IPMQCommands* commands, std::uint64_t queueId, const char* routingKey, std::uint64_t messageId, const unsigned char* message, std::size_t size, void* userPtr) {
				auto callback = (Callback*)userPtr;
				Commands commandsWrapper(commands);
				(*callback)(commandsWrapper, queueId, routingKey, messageId, message, size);
			},
			&callback
		);

		if (result != 0) {
			throw std::runtime_error("Failed to consume queue.");
		}
	}

	Producer::Producer(const std::string& path, const std::string& sharedMemoryFile, std::size_t sharedMemorySize) {
		mProducer = ipmq_producer_create(path.c_str(), sharedMemoryFile.c_str(), sharedMemorySize);
		if (mProducer == nullptr) {
			throw std::runtime_error("Failed to create producer.");
		}
	}

	Producer::~Producer() {
		if (mProducer != nullptr) {
			ipmq_producer_destroy(mProducer);
		}
	}

	void Producer::publishBytes(const std::string& routingKey, std::uint8_t* message, std::size_t messageSize) {
		if (ipmq_producer_publish_bytes(mProducer, routingKey.c_str(), message, messageSize) != 0) {
			throw std::runtime_error("Failed to publish message.");
		}
	}

	void Producer::publish(const std::string& routingKey, MemoryAllocation allocation) {
		if (ipmq_producer_publish(mProducer, routingKey.c_str(), allocation.underlying()) != 0) {
			throw std::runtime_error("Failed to publish message.");
		}
	}

	IPMQProducer* Producer::underlying() {
		return mProducer;
	}

	MemoryAllocation::MemoryAllocation(Producer& producer, std::size_t size) {
		mAllocation = ipmq_producer_allocate(producer.underlying(), size);
		if (mAllocation == nullptr) {
			throw std::runtime_error("Failed to allocate.");
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