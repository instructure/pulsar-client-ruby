#ifndef __PULSAR_RUBY_CLIENT_MESSAGE_HPP
#define __PULSAR_RUBY_CLIENT_MESSAGE_HPP

#include "rice/Hash.hpp"
#include "rice/Module.hpp"
#include "rice/String.hpp"
#include "rice/Data_Object.hpp"
#include <pulsar/Client.h>

namespace pulsar_rb {
  class MessageId {
  public:
    pulsar::MessageId _msgId;
    MessageId(const pulsar::MessageId& msgId) : _msgId(msgId) {};

    Rice::String toString();

    typedef Rice::Data_Object<MessageId> ptr;
  };

  class Message {
  public:
    pulsar::Message _msg;
    Message(const pulsar::Message& msg) : _msg(msg) {};
    Message(const std::string& data, Rice::Object opts);

    Rice::String getData();
    MessageId::ptr getMessageId();
    Rice::Hash getProperties();

    typedef Rice::Data_Object<Message> ptr;
  };
};

void bind_message(Rice::Module& module);

#endif
