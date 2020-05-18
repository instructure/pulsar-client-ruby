#include "rice/Hash.hpp"

#include "stringmap.hpp"

template<>
pulsar::StringMap from_ruby<pulsar::StringMap>(Rice::Object o)
{
  Rice::Hash h(o);
  pulsar::StringMap m;
  Rice::Hash::iterator it = h.begin();
  Rice::Hash::iterator end = h.end();
  for(; it != end; ++it) {
    m[it->key.to_s().str()] = Rice::Object(it->value).to_s().str();
  }
  return m;
}

template<>
Rice::Object to_ruby<pulsar::StringMap>(pulsar::StringMap const & m)
{
  Rice::Hash h = Rice::Hash();

  pulsar::StringMap::const_iterator it = m.begin();
  pulsar::StringMap::const_iterator end = m.end();
  for(; it != end; ++it) {
    h[Rice::String(it->first)] = Rice::String(it->second);
  }

  // Be clear that calling hash[x]= will have no effect.
  h.freeze();

  return h;
}
