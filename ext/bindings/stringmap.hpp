#ifndef __PULSAR_RUBY_CLIENT_STRINGMAP_HPP
#define __PULSAR_RUBY_CLIENT_STRINGMAP_HPP

#include "rice/Object.hpp"
#include <pulsar/Client.h>

template<>
pulsar::StringMap from_ruby<pulsar::StringMap>(Rice::Object o);

template<>
Rice::Object to_ruby<pulsar::StringMap>(pulsar::StringMap const & m);

#endif
