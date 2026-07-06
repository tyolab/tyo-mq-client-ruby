# tyo-mq-client-ruby

A Ruby client for **[tyo-mq](https://github.com/tyolab/tyo-mq)** — the
distributed pub/sub messaging service with durable delivery (ACK / retry /
dead-letter queue), MQTT-style topic wildcards, consumer groups, and
multi-tenant auth realms.

The client implements the Socket.IO v4 protocol directly on
[websocket-client-simple](https://github.com/ruby-jp/websocket-client-simple)
— no Socket.IO library dependency. Ruby 2.7+.

## Install

```bash
gem install tyo-mq-client
# or from source:
gem build tyo-mq-client.gemspec && gem install tyo-mq-client-*.gem
```

You'll need a running tyo-mq server — see the
[server repo](https://github.com/tyolab/tyo-mq).

## Quick start

```ruby
require 'tyo_mq_client'

# consume (durable + auto-ACK)
consumer = TyoMq::Client.new(host: 'localhost', port: 17352).connect
# with auth enabled on the server: auth_token: 'my-token'
consumer.register_consumer('email-service')
consumer.subscribe('order-service', 'order-placed', 'email-service',
                   durable: true, ack: true,
                   retry: { max_attempts: 3, delay: '5s', backoff: 'exponential' }) do |message, from, _ack, raw|
  puts "order event from #{from}: #{message.inspect}"
end

# produce
producer = TyoMq::Client.new(host: 'localhost', port: 17352).connect
producer.register_producer('order-service')
producer.produce('order-service', 'order-placed', { 'orderId' => 1001 })
```

A block that raises is **not** acknowledged — the server re-delivers on the
retry schedule and dead-letters the message when attempts run out. With
`manual_ack: true` (plus e.g. `ack_timeout: '30s'`) the block's third
argument is an `ack` lambda to call once the work truly succeeded.

## Topics, groups, broadcast

```ruby
# MQTT-style wildcards: + is one level, # is the rest
consumer.subscribe(nil, 'orders/+/status', 'dashboard', mode: 'topic') do |message, _from, _ack, raw|
  puts "#{raw['event']}: #{message.inspect}"
end

# consumer groups load-balance across workers
consumer.subscribe('dispatcher', 'jobs', 'worker-1', group: 'workers') { |job, *| work(job) }

# broadcast one copy to every realm member, or every group member
producer.broadcast('control', 'announcement', { 'notice' => 'maintenance' })
producer.broadcast('control', 'reload', {}, kind: 'group', group: 'workers')
```

Large messages are chunked automatically in both directions (256 KB frames),
matching the Node.js client. Anything the helpers don't cover is one
`emit(event, payload)` / `on(event) { ... }` away.

## Example

```bash
ruby examples/pubsub.rb localhost 17352
```

`examples/pubsub.rb` is a complete round trip (durable + auto-ACK), verified
against a live tyo-mq server.

## Other clients

Node.js (and browsers) ships with the [server package](https://github.com/tyolab/tyo-mq);
see also [Python](https://github.com/tyolab/tyo-mq-client-python),
[Go](https://github.com/tyolab/tyo-mq-client-go),
[Rust](https://github.com/tyolab/tyo-mq-client-rust),
[C/C++](https://github.com/tyolab/tyo-mq-client-cpp),
[Java](https://github.com/tyolab/tyo-mq-client-java), and
[C#](https://github.com/tyolab/tyo-mq-client-csharp).

## License

Apache-2.0. Built by [TYO Lab](https://tyo.com.au).
