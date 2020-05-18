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

RSpec.describe Pulsar::Message do
  describe "::new" do
    it "takes a string" do
      m = described_class.new("payload")
      expect(m.data).to eq("payload")
    end

    describe "properties" do
      it "takes properties" do
        m = described_class.new("payload", properties: {"a" => "1", "b" => "2"})
        expect(m.data).to eq("payload")
        expect(m.properties).to eq({"a" => "1", "b" => "2"})
      end

      it "stringifies non-string properties" do
        m = described_class.new("payload", properties: {
          "a" => 1,
          :b => [2],
        })
        expect(m.data).to eq("payload")
        expect(m.properties).to eq({"a" => "1", "b" => "[2]"})
      end

      it "takes a lot of properties" do
        m = described_class.new("payload", properties: {
          "a" => 1,
          "b" => [2],
          "c" => ("c" * 100000),
          "license" => File.read(File.expand_path("../../LICENSE", __dir__)),
        })
        expect(m.data).to eq("payload")
        expect(m.properties["license"]).to match(/Apache License/)
        expect(m.properties["license"]).to match(/limitations under the License/)
        expect(m.properties["c"]).to match(/^c{100000}$/)
        expect(m.properties.values_at("a", "b")).to eq(["1", "[2]"])
      end

      it "accepts nil properties" do
        m = described_class.new("payload", properties: nil)
        expect(m.properties).to eq({})
      end
    end

    describe "errors" do
      it "rejects second arg that is not a hash" do
        expect do
          described_class.new("payload", [1])
        end.to raise_exception(TypeError)
      end

      it "rejects unknown named arguments" do
        expect do
          described_class.new("payload", properties: {}, foo: "x")
        end.to raise_exception(ArgumentError, /Unknown keyword argument: foo/)
      end

      it "rejects properties that are not a hash" do
        expect do
          described_class.new("payload", properties: [])
        end.to raise_exception(TypeError)
      end
    end
  end
end
