#include "rice/Data_Type.hpp"
#include "rice/Constructor.hpp"
#include <pulsar/Client.h>

#include "message.hpp"

namespace pulsar_rb {

Rice::String MessageId::toString() {
  std::stringstream ss;
  ss << _msgId;
  return Rice::String(ss.str());
}

Message::Message(const std::string& data) {
  pulsar::MessageBuilder mb;
  mb.setContent(data);
  _msg = mb.build();
}

Message Message::fromMessage(const pulsar::Message& msg) {
  return Message(msg);
}

Rice::String Message::getData() {
  std::string str((const char*)_msg.getData(), _msg.getLength());
  return Rice::String(str);
}

MessageId::ptr Message::getMessageId() {
  pulsar::MessageId messageId = _msg.getMessageId();
  return MessageId::ptr(new MessageId(messageId));
}

}

using namespace Rice;

void bind_message(Module& module) {
  define_class_under<pulsar_rb::MessageId>(module, "MessageId")
    .define_constructor(Constructor<pulsar_rb::MessageId, const pulsar::MessageId&>())
    .define_method("to_s", &pulsar_rb::MessageId::toString)
    ;

  define_class_under<pulsar_rb::Message>(module, "Message")
    .define_constructor(Constructor<pulsar_rb::Message, const std::string&>())
    .define_singleton_method("from_message", &pulsar_rb::Message::fromMessage)
    .define_method("data", &pulsar_rb::Message::getData)
    .define_method("message_id", &pulsar_rb::Message::getMessageId)
    ;
}
