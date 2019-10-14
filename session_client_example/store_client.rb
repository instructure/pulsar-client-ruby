require "./session_client"

class StoreClient
  def with_session
    raise NotImplemented
  end

  def get(key)
    with_session { |session| _get(session, key) }
  end

  def put(key, value)
    with_session { |session| _put(session, key, value) }
  end

  def _put(session, key, value)
    session.select(:put)
    session.send(key)
    session.send(value)
    case session.branch
    when :success
      true
    when :failure
      false
    end
  end

  def _get(session, key)
    session.select(:get)
    session.send(key)
    case session.branch
    when :success
      session.receive
    when :failure
      nil
    end
  end

  class Pulsar < StoreClient
    def initialize
      @client = SessionClient::Pulsar.from_environment
    end

    def with_session
      @client.connect { |session| yield session }
    end
  end
end
