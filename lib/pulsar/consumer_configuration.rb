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
require 'pulsar/producer'

module Pulsar
  class ConsumerConfiguration
    # aligns with the pulsar::ConsumerType enum in the C++ library
    CONSUMER_TYPES = {
      :exclusive => Pulsar::ConsumerType::Exclusive,
      :shared => Pulsar::ConsumerType::Shared,
      :failover => Pulsar::ConsumerType::Failover,
      :key_shared => Pulsar::ConsumerType::KeyShared
    }

    module RubySideTweaks
      def initialize(config={})
        super()
        self.consumer_type = config[:consumer_type] if config.has_key?(:consumer_type)
      end

      def consumer_type
        enum_value = super
        CONSUMER_TYPES.invert[enum_value]
      end

      def consumer_type=(type)
        unless type.is_a?(Pulsar::ConsumerType)
          type = CONSUMER_TYPES[type]
          unless type
            raise ArgumentError, "unrecognized consumer_type"
          end
        end
        super(type)
      end
    end

    prepend RubySideTweaks
  end
end
