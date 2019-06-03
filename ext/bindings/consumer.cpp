#include "rice/Data_Type.hpp"
#include "rice/Constructor.hpp"
#include <pulsar/Client.h>
#include <ruby/thread.h>

#include "consumer.hpp"
#include "util.hpp"

namespace pulsar_rb {

typedef struct {
  pulsar::Consumer& consumer;
  pulsar::Message message;
  pulsar::Result result;
} consumer_receive_job;

void* consumer_receive_nogvl(void* jobPtr) {
  consumer_receive_job& job = *(consumer_receive_job*)jobPtr;
  job.result = job.consumer.receive(job.message);
  return nullptr;
}

pulsar::Message consumer_receive(pulsar::Consumer& consumer) {
  consumer_receive_job job = { consumer };
  rb_thread_call_without_gvl(&consumer_receive_nogvl, &job, RUBY_UBF_IO, nullptr);
  CheckResult(job.result);
  return job.message;
}

Message::ptr Consumer::receive() {
  pulsar::Message message = consumer_receive(_consumer);
  return Message::ptr(new Message(message));
}

void Consumer::acknowledge(const Message& message) {
  _consumer.acknowledgeAsync(message._msg, nullptr);
}

/* TODO [with future libpulsar version]
 * this is not yet available in libpulsar v2.3.2

void Consumer::negative_acknowledge(const Message& message) {
  _consumer.negativeAcknowledge(message._msg);
}

*/

}

using namespace Rice;

void bind_consumer(Module &module) {
  define_class_under<pulsar_rb::Consumer>(module, "Consumer")
    .define_constructor(Constructor<pulsar_rb::Consumer>())
    .define_method("receive", &pulsar_rb::Consumer::receive)
    .define_method("acknowledge", &pulsar_rb::Consumer::acknowledge)
    // TODO [with future libpulsar version]
    //.define_method("negative_acknowledge", &pulsar_rb::Consumer::negative_acknowledge)
    ;

  define_class_under<pulsar_rb::ConsumerConfiguration>(module, "ConsumerConfiguration")
    .define_constructor(Constructor<pulsar_rb::ConsumerConfiguration>())
    .define_method("consumer_type", &ConsumerConfiguration::getConsumerType)
    .define_method("consumer_type=", &ConsumerConfiguration::setConsumerType)
    .define_method("schema", &ConsumerConfiguration::getSchema)
    .define_method("schema=", &ConsumerConfiguration::setSchema)
    // TODO .define_method("message_listener", &ConsumerConfiguration_setMessageListener)
    .define_method("receiver_queue_size", &ConsumerConfiguration::getReceiverQueueSize)
    .define_method("receiver_queue_size=", &ConsumerConfiguration::setReceiverQueueSize)
    .define_method("max_total_receiver_queue_size_across_partitions", &ConsumerConfiguration::getMaxTotalReceiverQueueSizeAcrossPartitions)
    .define_method("max_total_receiver_queue_size_across_partitions=", &ConsumerConfiguration::setMaxTotalReceiverQueueSizeAcrossPartitions)
    .define_method("consumer_name", &ConsumerConfiguration::getConsumerName)
    .define_method("consumer_name=", &ConsumerConfiguration::setConsumerName)
    .define_method("unacked_messages_timeout_ms", &ConsumerConfiguration::getUnAckedMessagesTimeoutMs)
    .define_method("unacked_messages_timeout_ms=", &ConsumerConfiguration::setUnAckedMessagesTimeoutMs)
    // TODO [with future libpulsar version]
    // .define_method("negative_ack_redelivery_delay_ms", &ConsumerConfiguration::getNegativeAckRedeliveryDelayMs)
    // .define_method("negative_ack_redelivery_delay_ms=", &ConsumerConfiguration::setNegativeAckRedeliveryDelayMs)
    .define_method("broker_consumer_stats_cache_time_ms", &ConsumerConfiguration::getBrokerConsumerStatsCacheTimeInMs)
    .define_method("broker_consumer_stats_cache_time_ms=", &ConsumerConfiguration::setBrokerConsumerStatsCacheTimeInMs)
    .define_method("pattern_auto_discovery_period", &ConsumerConfiguration::getPatternAutoDiscoveryPeriod)
    .define_method("pattern_auto_discovery_period=", &ConsumerConfiguration::setPatternAutoDiscoveryPeriod)
    .define_method("read_compacted?", &ConsumerConfiguration::isReadCompacted)
    .define_method("read_compacted=", &ConsumerConfiguration::setReadCompacted)
    .define_method("subscription_initial_position", &ConsumerConfiguration::getSubscriptionInitialPosition)
    .define_method("subscription_initial_position=", &ConsumerConfiguration::setSubscriptionInitialPosition)
    .define_method("[]", &ConsumerConfiguration::getProperty)
    .define_method("[]=", &ConsumerConfiguration::setProperty)
    ;
}
