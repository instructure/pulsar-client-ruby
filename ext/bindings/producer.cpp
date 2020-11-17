#include "rice/Data_Type.hpp"
#include "rice/Constructor.hpp"
#include <pulsar/Client.h>
#include <ruby/thread.h>

#include "producer.hpp"
#include "util.hpp"

namespace pulsar_rb {

typedef struct {
  pulsar::Producer& producer;
  const pulsar::Message& message;
  pulsar::Result result;
} producer_send_task;

typedef struct {
  pulsar::Producer& producer;
  pulsar::Result result;
} producer_close_task;

void* producer_send_worker(void* taskPtr) {
  producer_send_task& task = *(producer_send_task*)taskPtr;
  task.result = task.producer.send(task.message);
  return nullptr;
}

void Producer::send(const Message& message) {
  producer_send_task task = { _producer, message._msg };
  rb_thread_call_without_gvl(&producer_send_worker, &task, RUBY_UBF_IO, nullptr);
  CheckResult(task.result);
}

void* producer_close_worker(void* taskPtr) {
  producer_close_task& task = *(producer_close_task*)taskPtr;
  task.result = task.producer.close();
  return nullptr;
}

void Producer::close() {
  producer_close_task task = { _producer };
  rb_thread_call_without_gvl(&producer_close_worker, &task, RUBY_UBF_IO, nullptr);
  CheckResult(task.result);
}

}

using namespace Rice;

void bind_producer(Module& module) {
  define_class_under<pulsar_rb::Producer>(module, "Producer")
    .define_constructor(Constructor<pulsar_rb::Producer>())
    .define_method("send", &pulsar_rb::Producer::send)
    .define_method("close", &pulsar_rb::Producer::close)
    ;

  define_class_under<pulsar_rb::ProducerConfiguration>(module, "ProducerConfiguration")
    .define_constructor(Constructor<pulsar_rb::ProducerConfiguration>())
    .define_method("producer_name", &ProducerConfiguration::getProducerName)
    .define_method("producer_name=", &ProducerConfiguration::setProducerName)
    // TODO .define_method("schema", &ProducerConfiguration::getSchema)
    // TODO .define_method("schema=", &ProducerConfiguration::setSchema)
    .define_method("send_timeout_millis", &ProducerConfiguration::getSendTimeout)
    .define_method("send_timeout_millis=", &ProducerConfiguration::setSendTimeout)
    .define_method("initial_sequence_id", &ProducerConfiguration::getInitialSequenceId)
    .define_method("initial_sequence_id=", &ProducerConfiguration::setInitialSequenceId)
    .define_method("compression_type", &ProducerConfiguration::getCompressionType)
    .define_method("compression_type=", &ProducerConfiguration::setCompressionType)
    .define_method("max_pending_messages", &ProducerConfiguration::getMaxPendingMessages)
    .define_method("max_pending_messages=", &ProducerConfiguration::setMaxPendingMessages)
    .define_method("max_pending_messages_across_partitions", &ProducerConfiguration::getMaxPendingMessagesAcrossPartitions)
    .define_method("max_pending_messages_across_partitions=", &ProducerConfiguration::setMaxPendingMessagesAcrossPartitions)
    .define_method("block_if_queue_full", &ProducerConfiguration::getBlockIfQueueFull)
    .define_method("block_if_queue_full=", &ProducerConfiguration::setBlockIfQueueFull)
    .define_method("partitions_routing_mode", &ProducerConfiguration::getPartitionsRoutingMode)
    .define_method("partitions_routing_mode=", &ProducerConfiguration::setPartitionsRoutingMode)
    .define_method("batching_enabled", &ProducerConfiguration::getBatchingEnabled)
    .define_method("batching_enabled=", &ProducerConfiguration::setBatchingEnabled)
    .define_method("batching_max_messages", &ProducerConfiguration::getBatchingMaxMessages)
    .define_method("batching_max_messages=", &ProducerConfiguration::setBatchingMaxMessages)
    .define_method("batching_max_allowed_size_in_bytes", &ProducerConfiguration::getBatchingMaxAllowedSizeInBytes)
    .define_method("batching_max_allowed_size_in_bytes=", &ProducerConfiguration::setBatchingMaxAllowedSizeInBytes)
    .define_method("batching_max_publish_delay_ms", &ProducerConfiguration::getBatchingMaxPublishDelayMs)
    .define_method("batching_max_publish_delay_ms=", &ProducerConfiguration::setBatchingMaxPublishDelayMs)
    .define_method("[]", &ProducerConfiguration::getProperty)
    .define_method("[]=", &ProducerConfiguration::setProperty);
}
