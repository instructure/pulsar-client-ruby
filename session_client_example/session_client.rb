require './protocol'
require './session'
require 'pulsar/client'
require 'json'

class SessionClient
  class Pulsar
    def self.from_environment(config={})
      namespace = config[:namespace] || ENV['PULSAR_SESSION_NAMESPACE']
      if namespace.nil?
        raise ArgumentError, "must provide a :namespace (or PULSAR_SESSION_NAMESPACE in environment)"
      end

      protocol = config[:protocol] || ENV['PULSAR_SESSION_PROTOCOL']
      if protocol.nil?
        raise ArgumentError, "must provide a :protocol (or PULSAR_SESSION_PROTOCOL in environment)"
      end

      protocol = "protocol.yml"
      self.new(namespace, protocol, config)
    end

    def initialize(namespace, protocol_file, config={})
      @threads = []
      @namespace = namespace
      @protocol = Protocol.load(protocol_file)
      @client = ::Pulsar::Client.from_environment(config)
    end

    def coordination_topic
      "#{@namespace}/coordination"
    end

    def random_session_topic
      "%s/session-%8x" % [@namespace, rand(2**32)]
    end

    def connect
      session_topic = random_session_topic
      producer = @client.create_producer(coordination_topic)
      producer.send({ topic: session_topic, protocol: @protocol.spec }.to_json)
      request(session_topic) { |session| yield session }
    end

    def request(topic)
      session = Session.new(@client, topic, 'client', @protocol)
      rv = yield session
      session.assert_finished
      rv
    end

    def listen
      threads = []
      consumer = @client.subscribe(coordination_topic, 'server')
      loop do
        begin
          msg = consumer.receive(100)
        rescue ::Pulsar::Error::Timeout
          next
        end
        puts "received session request"
        json = JSON.parse(msg.data)
        spec = json["protocol"]
        if @protocol.matches(spec)
          puts "accepting session"
          threads << accept(json["topic"]) { |session| yield session }
        else
          puts "rejecting session (protocol mismatch)"
          reject(json["topic"])
        end
        consumer.acknowledge(msg)
      end
      threads.join
    end

    def accept(topic)
      session = Session.new(@client, topic, 'server', @protocol.dual)
      Thread.new do
        yield session
        session.assert_finished
      end
    end

    def reject(topic)
      producer = @client.create_producer(topic)
      producer.send({ reject: 'true', role: 'server' }.to_json)
    end
  end
end
