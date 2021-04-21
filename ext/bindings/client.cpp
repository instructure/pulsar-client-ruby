#include "rice/Data_Type.hpp"
#include "rice/Constructor.hpp"
#include <pulsar/Client.h>
#include <ruby/thread.h>

#include "client.hpp"
#include "logger.hpp"
#include "util.hpp"
#include "vector.hpp"

namespace pulsar_rb {

ClientConfiguration::ClientConfiguration() : _config() {
}

void ClientConfiguration::setAuthFromToken(const std::string &token) {
  _config.setAuth(pulsar::AuthToken::createWithToken(token));
}

void ClientConfiguration::setAuthFromOAuth2Params(const std::string &authParamsString) {
  _config.setAuth(pulsar::AuthOauth2::create(authParamsString));
}

int ClientConfiguration::getOperationTimeoutSeconds() {
  return _config.getOperationTimeoutSeconds();
}

void ClientConfiguration::setOperationTimeoutSeconds(int timeout) {
  _config.setOperationTimeoutSeconds(timeout);
}

int ClientConfiguration::getIOThreads() {
  return _config.getIOThreads();
}

void ClientConfiguration::setIOThreads(int threads) {
  _config.setIOThreads(threads);
}

int ClientConfiguration::getMessageListenerThreads() {
  return _config.getMessageListenerThreads();
}

void ClientConfiguration::setMessageListenerThreads(int threads) {
  _config.setMessageListenerThreads(threads);
}

int ClientConfiguration::getConcurrentLookupRequest() {
  return _config.getConcurrentLookupRequest();
}

void ClientConfiguration::setConcurrentLookupRequest(int n) {
  _config.setConcurrentLookupRequest(n);
}

std::string ClientConfiguration::getLogConfFilePath() {
  return _config.getLogConfFilePath();
}

void ClientConfiguration::setLogConfFilePath(const std::string& path) {
  _config.setLogConfFilePath(path);
}

void ClientConfiguration::setSilentLogging(bool enable) {
  // The logger can only be set once, so if it's already on we cannot disable it.
  if (silentLogging) {
    if (!enable) {
      throw Rice::Exception(rb_eArgError, "silent_logging can only be set once");
    }
  }

  if (enable) {
    silentLogging = true;
    std::unique_ptr<LoggerFactory> loggerFactory = SilentLoggerFactory::create();
    _config.setLogger(loggerFactory.release());
  }
}

bool ClientConfiguration::getSilentLogging() {
  return silentLogging;
}

bool ClientConfiguration::isUseTls() {
  return _config.isUseTls();
}

void ClientConfiguration::setUseTls(bool enable) {
  _config.setUseTls(enable);
}

std::string ClientConfiguration::getTlsTrustCertsFilePath() {
  return _config.getTlsTrustCertsFilePath();
}

void ClientConfiguration::setTlsTrustCertsFilePath(const std::string& path) {
  _config.setTlsTrustCertsFilePath(path);
}

bool ClientConfiguration::isTlsAllowInsecureConnection() {
  return _config.isTlsAllowInsecureConnection();
}

void ClientConfiguration::setTlsAllowInsecureConnection(bool enable) {
  _config.setTlsAllowInsecureConnection(enable);
}

bool ClientConfiguration::isValidateHostName() {
  return _config.isValidateHostName();
}

void ClientConfiguration::setValidateHostName(bool enable) {
  _config.setValidateHostName(enable);
}

Client::Client(Rice::String service_url, const ClientConfiguration& config) : _client(service_url.str(), config._config) {
}

typedef struct {
  pulsar::Client& client;
  const Rice::String& topic;
  const pulsar::ProducerConfiguration& config;
  pulsar::Producer producer;
  pulsar::Result result;
} client_create_producer_task;

void* client_create_producer_worker(void* taskPtr) {
  client_create_producer_task& task = *(client_create_producer_task*)taskPtr;
  task.result = task.client.createProducer(task.topic.str(), task.config, task.producer);
  return nullptr;
}

Producer::ptr Client::create_producer(Rice::String topic, const ProducerConfiguration& config) {
  client_create_producer_task task = { _client, topic, config };
  rb_thread_call_without_gvl(&client_create_producer_worker, &task, RUBY_UBF_IO, nullptr);
  CheckResult(task.result);
  return Producer::ptr(new Producer(task.producer));
}

typedef struct {
  pulsar::Client& client;
  const Rice::Array& topics;
  const Rice::String& subscriptionName;
  const pulsar::ConsumerConfiguration& config;
  pulsar::Consumer consumer;
  pulsar::Result result;
} client_subscribe_task;

void* client_subscribe_worker(void* taskPtr) {
  client_subscribe_task& task = *(client_subscribe_task*)taskPtr;

  const std::vector<std::string>& topics = from_ruby<std::vector<std::string>>(task.topics);
  switch(topics.size()) {
    case 0:
      throw Rice::Exception(rb_eArgError, "Must have at least one topic");
      break;
    case 1: {
      // Skip the MultiTopicsConsumer if there's only one.
      task.result = task.client.subscribe(topics[0], task.subscriptionName.str(), task.config, task.consumer);
      break;
    }
    default: {
      task.result = task.client.subscribe(topics, task.subscriptionName.str(), task.config, task.consumer);
      break;
    }
  }
  return nullptr;
}

Consumer::ptr Client::subscribe(Rice::Array topics, Rice::String subscriptionName, const ConsumerConfiguration& config) {
  client_subscribe_task task = { _client, topics, subscriptionName, config };
  rb_thread_call_without_gvl(&client_subscribe_worker, &task, RUBY_UBF_IO, nullptr);
  CheckResult(task.result);
  return Consumer::ptr(new Consumer(task.consumer));
}

typedef struct {
  pulsar::Client& client;
  pulsar::Result result;
} client_close_task;

void* client_close_worker(void* taskPtr) {
  client_close_task& task = *(client_close_task*)taskPtr;
  task.result = task.client.close();
  return nullptr;
}

void Client::close() {
  client_close_task task = { _client };
  rb_thread_call_without_gvl(&client_close_worker, &task, RUBY_UBF_IO, nullptr);
  CheckResult(task.result);
}

}

using namespace Rice;

void bind_client(Module& module) {
  define_class_under<pulsar_rb::Client>(module, "Client")
    .define_constructor(Constructor<pulsar_rb::Client, const std::string&, const pulsar_rb::ClientConfiguration&>())
    .define_method("create_producer", &pulsar_rb::Client::create_producer)
    .define_method("subscribe", &pulsar_rb::Client::subscribe)
    .define_method("close", &pulsar_rb::Client::close)
    ;

  define_class_under<pulsar_rb::ClientConfiguration>(module, "ClientConfiguration")
    .define_constructor(Constructor<pulsar_rb::ClientConfiguration>())
    .define_method("authentication_token=", &pulsar_rb::ClientConfiguration::setAuthFromToken)
    .define_method("authentication_oauth2_params=", &pulsar_rb::ClientConfiguration::setAuthFromOAuth2Params)
    .define_method("operation_timeout_seconds", &pulsar_rb::ClientConfiguration::getOperationTimeoutSeconds)
    .define_method("operation_timeout_seconds=", &pulsar_rb::ClientConfiguration::setOperationTimeoutSeconds)
    .define_method("io_threads", &pulsar_rb::ClientConfiguration::getIOThreads)
    .define_method("io_threads=", &pulsar_rb::ClientConfiguration::setIOThreads)
    .define_method("message_listener_threads", &pulsar_rb::ClientConfiguration::getMessageListenerThreads)
    .define_method("message_listener_threads=", &pulsar_rb::ClientConfiguration::setMessageListenerThreads)
    .define_method("concurrent_lookup_requests", &pulsar_rb::ClientConfiguration::getConcurrentLookupRequest)
    .define_method("concurrent_lookup_requests=", &pulsar_rb::ClientConfiguration::setConcurrentLookupRequest)
    .define_method("log_conf_file_path", &pulsar_rb::ClientConfiguration::getLogConfFilePath)
    .define_method("log_conf_file_path=", &pulsar_rb::ClientConfiguration::setLogConfFilePath)
    .define_method("silent_logging?", &pulsar_rb::ClientConfiguration::getSilentLogging)
    .define_method("silent_logging=", &pulsar_rb::ClientConfiguration::setSilentLogging)
    .define_method("use_tls?", &pulsar_rb::ClientConfiguration::isUseTls)
    .define_method("use_tls=", &pulsar_rb::ClientConfiguration::setUseTls)
    .define_method("tls_trust_certs_file_path", &pulsar_rb::ClientConfiguration::getTlsTrustCertsFilePath)
    .define_method("tls_trust_certs_file_path=", &pulsar_rb::ClientConfiguration::setTlsTrustCertsFilePath)
    .define_method("tls_allow_insecure_connection?", &pulsar_rb::ClientConfiguration::isTlsAllowInsecureConnection)
    .define_method("tls_allow_insecure_connection=", &pulsar_rb::ClientConfiguration::setTlsAllowInsecureConnection)
    .define_method("tls_validate_hostname?", &pulsar_rb::ClientConfiguration::isValidateHostName)
    .define_method("tls_validate_hostname=", &pulsar_rb::ClientConfiguration::setValidateHostName)
    ;
}
