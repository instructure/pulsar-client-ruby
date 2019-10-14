require './protocol'

class SessionClient
  class Session
    def initialize(client, topic, role, protocol)
      @role = role
      @producer = client.create_producer(topic)
      @consumer = client.subscribe(topic, role)
      @protocol = protocol
    end

    def assert_finished
      raise Protocol::Violation unless @protocol == Protocol::End
    end

    def select(label)
      @protocol = @protocol.select(label) { _send(select: label.to_s) }
      puts "selected branch #{label.inspect}"
    end

    def branch
      label, @protocol = @protocol.branch { _recv["select"].to_sym }
      puts "received branch selection #{label.inspect}"
      label
    end

    def send(value)
      @protocol = @protocol.send(value) { _send(value: value) }
      puts "sent value #{value.inspect}"
    end

    def receive
      value, @protocol = @protocol.receive { _recv["value"] }
      puts "received value #{value.inspect}"
      value
    end

    def _send(data)
      puts "sending #{data.merge(role: @role).inspect} as json"
      @producer.send(data.merge(role: @role).to_json)
    end

    def _recv
      loop do
        begin
          msg = @consumer.receive(100)
        rescue ::Pulsar::Error::Timeout
          next
        end
        @consumer.acknowledge(msg)
        json = JSON.parse(msg.data)
        role = json.delete("role")
        return json unless role == @role
      end
    end
  end
end
