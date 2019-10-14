require "./session_client"

class StoreServer
  def initialize
    # TODO use thread-safe data structure
    @store = {}
  end

  def get(session)
    key = session.receive
    puts "reading key #{key} from internal store"
    value = @store[key]
    puts "got #{value.inspect} for key #{key} from internal store"
    if value.nil?
      session.select(:failure)
    else
      session.select(:success)
      session.send(value)
    end
  rescue => e
    puts e.inspect
    session.select(:failure)
  end

  def put(session)
    key = session.receive
    value = session.receive
    if key == "bad key"
      session.select(:failure)
    else
      @store[key] = value
      session.select(:success)
    end
  end

  def exec(session)
    case session.branch
    when :get then get(session)
    when :put then put(session)
    end
  end

  def listen
    each_session { |session| exec(session) }
  end

  def each_session
    raise NotImplemented
  end

  class Pulsar < StoreServer
    def initialize
      super
      @client = SessionClient::Pulsar.from_environment
    end

    def each_session
      @client.listen { |session| yield session }
    end
  end
end
