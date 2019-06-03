#ifndef __PULSAR_RUBY_CLIENT_SCHEMA_HPP
#define __PULSAR_RUBY_CLIENT_SCHEMA_HPP

#include "rice/Module.hpp"
#include "rice/Data_Object.hpp"
#include <pulsar/Client.h>

namespace pulsar_rb {
  // direct typedef instead of wrapping because implementations don't need any
  // wrapping. but still re-namespaced for consistency
  typedef pulsar::SchemaInfo SchemaInfo;
};

void bind_schema(Rice::Module& module);

#endif
