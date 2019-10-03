#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'pulsar/client/version'
require 'pulsar/bindings'
require 'pulsar/client_configuration'
require 'pulsar/consumer'
require 'pulsar/consumer_configuration'
require 'pulsar/producer'

module Pulsar
  class Client
    module RubySideTweaks
      def initialize(service_url, config=nil)
        config = Pulsar::ClientConfiguration.from(config)
        super(service_url, config)
      end

      def create_producer(topic, config=nil)
        config ||= Pulsar::ProducerConfiguration.new
        super(topic, config)
      end

      def subscribe(topic, subscription_name, config=nil)
        config ||= Pulsar::ConsumerConfiguration.new
        super(topic, subscription_name, config)
      end
    end

    prepend RubySideTweaks

    def self.sufficient_environment?(config={})
      config[:broker_uri] || ENV['PULSAR_BROKER_URI']
    end

    def self.from_environment(config={})
      broker_uri = config[:broker_uri] || ENV['PULSAR_BROKER_URI']
      config = Pulsar::ClientConfiguration.from_environment(config)
      new(broker_uri, config)
    end
  end
end
