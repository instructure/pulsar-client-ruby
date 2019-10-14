require "./store_client"

print "key: "
key = gets.chomp
print "value: "
value = gets.chomp

client = StoreClient::Pulsar.new
if client.put(key, value)
  puts "wrote #{value.inspect} to key #{key.inspect}"
else
  puts "failed to write to key #{key.inspect}"
end
