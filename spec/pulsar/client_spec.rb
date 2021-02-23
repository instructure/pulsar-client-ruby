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

RSpec.describe Pulsar::Client do
  it "has a version number" do
    expect(Pulsar::Client::VERSION).not_to be nil
  end

  context 'with pulsar' do
    let(:broker_uri) { ENV['PULSAR_BROKER_URI'] }
    let(:namespace) { ENV['PULSAR_CLIENT_RUBY_TEST_NAMESPACE'].to_s.sub(%r{^[-a-z]:/+}, '') }
    let(:configured?) { broker_uri && !namespace.empty? }
    let(:client) { Pulsar::Client.from_environment(broker_uri: broker_uri, silent_logging: !!ENV['PULSAR_CLIENT_SILENT_LOGGING']) }
    let(:topic) { "non-persistent://#{namespace}/test#{sprintf "%06d", rand(1_000_000)}" }
    let(:producer) { client.create_producer(topic) }
    let(:subscription_name) { "#{topic}-consumer" }
    let(:timeout_ms) { 10_000 }

    before(:each) do
      skip('Live Pulsar tests not configured: Set PULSAR_CLIENT_RUBY_TEST_NAMESPACE to enable') unless configured?
    end

    after(:each) do
      # Close any producers/consumers to avoid test pollution.
      client.close
    end

    it "can consume a single topic" do
      consumer = client.subscribe(topic, subscription_name)
      t = Thread.new { consumer.receive(timeout_ms) }
      client.create_producer(topic).send("single",
                                         ordering_key: "order",
                                         partition_key: "a key",
                                         properties: {foo: "bar"})
      message = t.join.value

      expect(message.data).to eq("single")
      expect(message.topic).to eq(topic)

      aggregate_failures "message attributes" do
        expect(message.ordering_key).to eq("order")
        expect(message.partition_key).to eq("a key")
        expect(message.properties).to eq({"foo" => "bar"})
        expect(message.properties["foo"]).to eq("bar")
      end
    end

    it "can consume multiple topics" do
      topics = [topic, "#{topic}.2"]
      consumer = client.subscribe(topics, subscription_name)
      t = Thread.new { topics.map { consumer.receive(timeout_ms).data } }
      topics.each.with_index do |t, i|
        client.create_producer(t).send("#{t} #{i}")
      end
      expect(t.join.value).to eq(topics.map.with_index { |t, i| "#{t} #{i}" })
    end

    it "errors with zero topics" do
      expect {
        client.subscribe([], subscription_name)
      }.to raise_error(ArgumentError, /at least one topic/)
    end
  end
end
