#ifndef __PULSAR_RUBY_CLIENT_CONSUMER_HPP
#define __PULSAR_RUBY_CLIENT_CONSUMER_HPP

#include "rice/Module.hpp"
#include "rice/Data_Object.hpp"
#include <pulsar/Client.h>

#include "message.hpp"

namespace pulsar_rb {
  class Consumer {
  public:
    pulsar::Consumer _consumer;
    Consumer() {};
    Consumer(const pulsar::Consumer& consumer) : _consumer(consumer) {}

    Message::ptr receive(unsigned int timeout_ms=0);
    void acknowledge(const Message& message);
    void negative_acknowledge(const Message& message);
    void close();

    typedef Rice::Data_Object<Consumer> ptr;
  };

  // direct typedef instead of wrapping because implementations don't need any
  // wrapping. but still re-namespaced for consistency
  typedef pulsar::ConsumerConfiguration ConsumerConfiguration;
};

void bind_consumer(Rice::Module& module);

#endif
