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

Message::Message(const std::string& data, const std::string& partitionKey) {
  buildMessage(data, partitionKey);
}

// pulsar::Message is immutable and must be rebuilt each time properties change
void Message::buildMessage(const std::string& data, const std::string& partitionKey) {
  pulsar::MessageBuilder mb;
  mb.setContent(data);
  // setting this to the empty string effectively "clears" the key
  mb.setPartitionKey(partitionKey);
  _msg = mb.build();
}

Rice::String Message::getData() {
  std::string str((const char*)_msg.getData(), _msg.getLength());
  return Rice::String(str);
}

MessageId::ptr Message::getMessageId() {
  pulsar::MessageId messageId = _msg.getMessageId();
  return MessageId::ptr(new MessageId(messageId));
}

Rice::String Message::getPartitionKey() {
  return Rice::String(_msg.getPartitionKey());
}

void Message::setPartitionKey(const std::string& partitionKey) {
  std::string str((const char*)_msg.getData(), _msg.getLength());
  buildMessage(str, partitionKey);
}

}

using namespace Rice;

void bind_message(Module& module) {
  define_class_under<pulsar_rb::MessageId>(module, "MessageId")
    .define_constructor(Constructor<pulsar_rb::MessageId, const pulsar::MessageId&>())
    .define_method("to_s", &pulsar_rb::MessageId::toString)
    ;

  define_class_under<pulsar_rb::Message>(module, "Message")
    .define_constructor(Constructor<pulsar_rb::Message, const std::string&, const std::string&>(), (Arg("message"), Arg("partition_key") = ""))
    .define_method("data", &pulsar_rb::Message::getData)
    .define_method("message_id", &pulsar_rb::Message::getMessageId)
    .define_method("partition_key", &pulsar_rb::Message::getPartitionKey)
    .define_method("partition_key=", &pulsar_rb::Message::setPartitionKey)
    ;
}
