#ifndef __PULSAR_RUBY_CLIENT_CLIENT_HPP
#define __PULSAR_RUBY_CLIENT_CLIENT_HPP

#include "rice/Module.hpp"
#include "rice/String.hpp"
#include "rice/Data_Object.hpp"
#include <pulsar/Client.h>

#include "producer.hpp"
#include "consumer.hpp"

namespace pulsar_rb {
  class ClientConfiguration {
  public:
    pulsar::ClientConfiguration _config;
    bool silentLogging = false;
    ClientConfiguration();

    void setAuthFromToken(const std::string &token);
    void setAuthFromOAuth2Params(const std::string &authParamsString);
    int getOperationTimeoutSeconds();
    void setOperationTimeoutSeconds(int timeout);
    int getIOThreads();
    void setIOThreads(int threads);
    int getMessageListenerThreads();
    void setMessageListenerThreads(int threads);
    int getConcurrentLookupRequest();
    void setConcurrentLookupRequest(int n);
    std::string getLogConfFilePath();
    void setLogConfFilePath(const std::string& path);
    void setSilentLogging(bool);
    bool getSilentLogging();
    bool isUseTls();
    void setUseTls(bool enable);
    std::string getTlsTrustCertsFilePath();
    void setTlsTrustCertsFilePath(const std::string& path);
    bool isTlsAllowInsecureConnection();
    void setTlsAllowInsecureConnection(bool enable);
    bool isValidateHostName();
    void setValidateHostName(bool enable);

    typedef Rice::Data_Object<ClientConfiguration> ptr;
  };

  class Client {
  public:
    pulsar::Client _client;
    Client(Rice::String service_url, const ClientConfiguration& config);

    Producer::ptr create_producer(Rice::String topic, const ProducerConfiguration& config);
    Consumer::ptr subscribe(Rice::Array topics, Rice::String subscriptionName, const ConsumerConfiguration& config);
    void close();

    typedef Rice::Data_Object<Client> ptr;
  };
};

void bind_client(Rice::Module &module);

#endif
