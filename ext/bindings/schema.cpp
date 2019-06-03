#include "rice/Data_Type.hpp"
#include "rice/Constructor.hpp"
#include <pulsar/Client.h>

#include "schema.hpp"

using namespace Rice;

void bind_schema(Module &module) {
  define_class_under<pulsar_rb::SchemaInfo>(module, "SchemaInfo")
    .define_constructor(Constructor<pulsar_rb::SchemaInfo, pulsar::SchemaType, const std::string&, const std::string&>())
    .define_method("schema_type", &pulsar_rb::SchemaInfo::getSchemaType)
    .define_method("name", &pulsar_rb::SchemaInfo::getName)
    .define_method("schema", &pulsar_rb::SchemaInfo::getSchema)
    ;
}
