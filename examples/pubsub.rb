# frozen_string_literal: true

# A minimal tyo-mq round trip: produce on one connection, consume (durable,
# auto-ACK) on another.
#
# Start a server first (see https://github.com/tyolab/tyo-mq), then:
#
#     ruby examples/pubsub.rb [host] [port]

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'tyo_mq_client'

host = ARGV[0] || 'localhost'
port = (ARGV[1] || TyoMq::DEFAULT_PORT).to_i

# with auth enabled on the server: auth_token: 'my-token'
consumer = TyoMq::Client.new(host: host, port: port).connect
consumer.register_consumer('ruby-listener')

queue = Queue.new
consumer.subscribe('ruby-example', 'order-placed', 'ruby-listener',
                   durable: true, ack: true, # auto-ACKed after the block returns
                   retry: { max_attempts: 3, delay: '5s', backoff: 'exponential' }) do |message, from, _ack, raw|
  puts "received from #{from}: #{message.inspect} (msgId: #{raw['msgId']})"
  queue << :ok
end

producer = TyoMq::Client.new(host: host, port: port).connect
producer.register_producer('ruby-example')

sleep 0.4 # let the subscription register
producer.produce('ruby-example', 'order-placed', { 'orderId' => 1001, 'total' => 129.0 })

Thread.new do
  sleep 10
  queue << :timeout
end
ok = queue.pop == :ok

producer.disconnect
consumer.disconnect

puts(ok ? 'round trip OK' : 'no message received before timeout')
exit(ok ? 0 : 1)
