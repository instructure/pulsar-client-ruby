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

require 'pulsar/bindings'

module Pulsar
  class Consumer
    class ListenerToken
      def initialize
        @active = true
      end

      def finish
        @active = false
      end

      def active?
        @active
      end
    end

    def listen
      listener = ListenerToken.new
      while listener.active?
        msg = receive
        yield msg.data, msg.message_id, lambda { listener.finish }
        acknowledge(msg)
      end
    end

    def listen_in_thread
      Thread.new { listen {|*args| yield *args }}
    end
  end
end
