require "./store_client"

print "key: "
key = gets.chomp

client = StoreClient::Pulsar.new
value = client.get(key)
if value
  puts "read value #{value.inspect} from key #{key.inspect}"
else
  puts "failed to get key #{key.inspect}"
end
