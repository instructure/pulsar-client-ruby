require "./store_server"

server = StoreServer::Pulsar.new
server.listen
