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

RSpec.describe Pulsar::Producer do
  describe "side tweaks" do
    describe "#send" do
      class ProducerTest
        def send(message)
          message
        end
        prepend Pulsar::Producer::RubySideTweaks
      end

      context "message" do
        it "passes Message through" do
          m = Pulsar::Message.new("payload")
          expect(Pulsar::Message).not_to receive(:new)
          expect(ProducerTest.new.send(m).data).to eq("payload")
        end

        it "warns about unused arguments" do
          m = Pulsar::Message.new("payload")
          test = ProducerTest.new
          expect(test).to receive(:warn).with(matching(/Ignoring options \((properties|foo), (properties|foo)\)/))
          expect(test.send(m, properties: {k: "v"}, foo: "bar").data).to eq("payload")
        end
      end

      it "creates Message from single string arg" do
        m = ProducerTest.new.send("payload")
        expect(m.data).to eq("payload")
      end

      describe "options" do
        subject {
          ProducerTest.new.send(
            "payload",
            properties: {"k" => "v"},
          )
        }

        it "sets data" do
          expect(subject.data).to eq("payload")
        end

        it "sets properties" do
          expect(subject.properties).to eq({"k" => "v"})
        end
      end
    end
  end
end
