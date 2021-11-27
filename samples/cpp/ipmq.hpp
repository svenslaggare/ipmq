#pragma once
#include <functional>

#include <ipmq.h>

namespace ipmq {
	void enableLogging();

	class Commands {
	private:
		IPMQCommands* mCommands;
	public:
		explicit Commands(IPMQCommands* commands);

		void acknowledgment(std::uint64_t queueId, std::uint64_t messageId);
		void negativeAcknowledgment(std::uint64_t queueId, std::uint64_t messageId);
		void stopConsume(std::uint64_t queueId);
	};

	class Consumer {
	private:
		IPMQConsumer* mConsumer;
	public:
		explicit Consumer(const std::string& path);

		~Consumer();

		Consumer(const Consumer&) = delete;
		Consumer& operator=(const Consumer&) = delete;

		void createQueue(const std::string& name, bool autoDelete = true, double ttl = -1.0);
		void bindQueue(const std::string& name, const std::string& pattern);

		using Callback = std::function<void (Commands&, uint64_t, const char*, uint64_t, const uint8_t*, uintptr_t)>;
		void startConsumeQueue(const std::string& name, Callback callback);
	};

	class Producer;

	class MemoryAllocation {
	private:
		IPMQMemoryAllocation* mAllocation;
	public:
		explicit MemoryAllocation(Producer& producer, std::size_t size);

		~MemoryAllocation();

		MemoryAllocation(const MemoryAllocation&) = delete;
		MemoryAllocation& operator=(const MemoryAllocation&) = delete;

		MemoryAllocation(MemoryAllocation&&) noexcept;
		MemoryAllocation& operator=(MemoryAllocation&&) noexcept;

		std::uint8_t* data();

		IPMQMemoryAllocation* underlying();
	};

	class Producer {
	private:
		IPMQProducer* mProducer;
	public:
		explicit Producer(const std::string& path, const std::string& sharedMemoryFile, std::size_t sharedMemorySize);

		~Producer();

		Producer(const Producer&) = delete;
		Producer& operator=(const Producer&) = delete;

		void publishBytes(const std::string& routingKey, std::uint8_t* message, std::size_t messageSize);
		void publish(const std::string& routingKey, MemoryAllocation allocation);

		IPMQProducer* underlying();
	};
}