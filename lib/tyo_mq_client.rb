# frozen_string_literal: true

# A Ruby client for tyo-mq (https://github.com/tyolab/tyo-mq) — the
# distributed pub/sub messaging service with durable delivery (ACK / retry /
# dead-letter queue), MQTT-style topic wildcards, consumer groups, and
# multi-tenant auth realms.
#
# Transport: Engine.IO v4 over WebSocket, then Socket.IO v4 events
# (wire format: 42["event_name",{...payload...}]), implemented directly on
# websocket-client-simple — no Socket.IO library dependency.

require 'json'
require 'securerandom'
require 'websocket-client-simple'

require_relative 'tyo_mq_client/version'

module TyoMq
  DEFAULT_PORT = 17_352
  ALL_PRODUCERS = 'TYO-MQ-ALL'
  # Must match Publisher.CHUNK_SIZE in the JS server/client (256 KB).
  CHUNK_SIZE = 256 * 1024

  # The Socket.IO event on which deliveries for a subscription arrive.
  #   scope nil/"" (default) -> "CONSUME-<lower(producer-event)>"
  #   scope "all"            -> "CONSUME-<lower(producer)>-TM-ALL"
  def self.consume_event_name(producer, event, scope = nil)
    return "CONSUME-#{producer.downcase}-TM-ALL" if scope == 'all'

    "CONSUME-#{"#{producer}-#{event}".downcase}"
  end

  # One tyo-mq connection. A Client can act as a producer, a consumer, or
  # both; create one per logical service identity.
  #
  #   client = TyoMq::Client.new(host: 'localhost', port: 17352)
  #   client.connect
  #   client.register_producer('order-service')
  #   client.produce('order-service', 'order-placed', { orderId: 1001 })
  class Client
    def initialize(host: 'localhost', port: DEFAULT_PORT, protocol: 'http',
                   auth_token: nil)
      @host = host
      @port = port
      @secure = protocol == 'https' || protocol == 'wss'
      @auth_token = auth_token

      @handlers = Hash.new { |h, k| h[k] = [] }
      @handlers_mutex = Mutex.new

      @state_mutex = Mutex.new
      @state_cv = ConditionVariable.new
      @connected = false
      @auth_state = 0 # 0 pending, 1 ok, -1 failed

      @chunks = {}
    end

    # Connects and blocks until the Socket.IO namespace is joined (and the
    # client is authenticated, when auth_token was given). Raises on timeout.
    def connect(timeout: 10)
      scheme = @secure ? 'wss' : 'ws'
      url = "#{scheme}://#{@host}:#{@port}/socket.io/?EIO=4&transport=websocket"

      client = self
      @ws = WebSocket::Client::Simple.connect(url)
      @ws.on(:message) { |msg| client.send(:handle_frame, msg.data.to_s) }
      @ws.on(:close) { client.send(:mark_disconnected) }
      @ws.on(:error) { |_e| } # surfaced by the timeout below when fatal

      @state_mutex.synchronize do
        @state_cv.wait(@state_mutex, timeout) unless @connected
        raise "timed out connecting to #{url}" unless @connected
      end

      authenticate!(timeout) if @auth_token
      self
    end

    def connected?
      @connected
    end

    # Registers a handler for a raw Socket.IO event name.
    def on(event, &handler)
      @handlers_mutex.synchronize { @handlers[event] << handler }
    end

    # Emits a raw Socket.IO event.
    def emit(event, payload)
      @ws.send("42#{JSON.generate([event, payload])}")
    end

    # -- high-level helpers ---------------------------------------------------

    def register_producer(name)
      emit('PRODUCER', { 'name' => name })
    end

    # The name doubles as the durable consumer identity: reconnect with the
    # same name to replay queued messages of a durable subscription.
    def register_consumer(name)
      emit('CONSUMER', { 'name' => name, 'id' => name, 'consumer_id' => name })
    end

    # Publishes one fire-and-forget message. Large payloads are split into
    # PRODUCE_CHUNK frames automatically.
    def produce(from, event, message)
      payload = { 'event' => event, 'message' => message, 'from' => from }
      full = JSON.generate(payload)
      return emit('PRODUCE', payload) if full.bytesize <= CHUNK_SIZE

      total = (full.bytesize + CHUNK_SIZE - 1) / CHUNK_SIZE
      transfer_id = SecureRandom.hex(12)
      total.times do |i|
        emit('PRODUCE_CHUNK',
             'transferId' => transfer_id, 'index' => i, 'total' => total,
             'data' => full.byteslice(i * CHUNK_SIZE, CHUNK_SIZE))
      end
    end

    # Broadcasts one copy to every realm member (kind: 'realm') or every
    # member of a consumer group (kind: 'group').
    def broadcast(from, event, message, kind: 'realm', group: nil)
      payload = {
        'event' => event, 'message' => message, 'from' => from,
        'method' => 'broadcast',
        'broadcast' => kind == 'group' ? 'group' : 'realm'
      }
      payload['group'] = group if group
      emit('PRODUCE', payload)
    end

    # Acknowledges one ACK-enabled delivery.
    def ack(msg_id)
      emit('ACK', { 'msgId' => msg_id })
    end

    # Subscribes +consumer+ to +event+ from +producer+ and dispatches
    # deliveries to the block as (message, from, ack, raw).
    #
    # Options (all optional, matching the other clients):
    #   durable: true, ack: true, manual_ack: true, ack_timeout: '30s',
    #   retry: { max_attempts: 3, delay: '5s', backoff: 'exponential' },
    #   mode: 'topic', group: 'workers'
    #
    # With ack and not manual_ack, deliveries are acknowledged automatically
    # after the block returns without raising. mode: 'topic' treats +event+
    # as an MQTT-style pattern (+ one level, # the rest); pass
    # TyoMq::ALL_PRODUCERS (or nil) as the producer.
    def subscribe(producer, event, consumer, options = {}, &handler)
      producer = ALL_PRODUCERS if producer.nil? && options[:mode] == 'topic'

      payload = {
        'event' => event, 'producer' => producer, 'consumer' => consumer,
        'scope' => 'default', 'consumer_id' => consumer
      }
      payload['durable'] = true if options[:durable]
      payload['ack'] = true if options[:ack] || options[:manual_ack]
      payload['manual_ack'] = true if options[:manual_ack]
      payload['ack_timeout'] = options[:ack_timeout] if options[:ack_timeout]
      payload['retry'] = options[:retry] if options[:retry]
      payload['mode'] = options[:mode] if options[:mode]
      payload['group'] = options[:group] if options[:group]

      auto_ack = (options[:ack] || options[:manual_ack]) && !options[:manual_ack]

      on(TyoMq.consume_event_name(producer, event)) do |obj|
        message = obj.is_a?(Hash) ? obj['message'] : obj
        from = obj.is_a?(Hash) ? obj['from'] : nil
        msg_id = obj.is_a?(Hash) ? (obj['msgId'] || obj['msg_id']) : nil

        acked = false
        ack_fn = lambda do
          next if msg_id.nil? || acked

          acked = true
          ack(msg_id)
        end

        begin
          handler.call(message, from, ack_fn, obj)
        rescue StandardError => e
          # No auto-ACK on a failed handler: the server retries on its
          # schedule and dead-letters when attempts are exhausted.
          warn "tyo-mq: consume handler failed: #{e.message}"
          next
        end
        ack_fn.call if auto_ack
      end

      emit('SUBSCRIBE', payload)
    end

    def disconnect
      @ws&.close
      mark_disconnected
    end

    private

    def authenticate!(timeout)
      client = self
      on('AUTH_OK') { |_p| client.send(:set_auth_state, 1) }
      on('AUTH_FAIL') { |_p| client.send(:set_auth_state, -1) }
      emit('AUTHENTICATION', { 'token' => @auth_token })

      @state_mutex.synchronize do
        @state_cv.wait(@state_mutex, timeout) if @auth_state.zero?
        raise 'authentication timed out' if @auth_state.zero?
        raise 'authentication failed' if @auth_state == -1
      end
    end

    def set_auth_state(value)
      @state_mutex.synchronize do
        @auth_state = value
        @state_cv.broadcast
      end
    end

    def mark_disconnected
      @state_mutex.synchronize do
        @connected = false
        @chunks.clear
      end
    end

    def handle_frame(frame)
      return if frame.empty?

      case frame[0]
      when '0' # Engine.IO OPEN — reply with Socket.IO CONNECT
        @ws.send('40')
      when '2' # Engine.IO PING -> PONG
        @ws.send('3')
      when '4' # Engine.IO MESSAGE -> Socket.IO packet
        case frame[1]
        when '0' # Socket.IO CONNECTED
          @state_mutex.synchronize do
            @connected = true
            @state_cv.broadcast
          end
        when '2' # Socket.IO EVENT
          dispatch(frame[2..])
        end
      end
    end

    def dispatch(data)
      arr = begin
        JSON.parse(data)
      rescue JSON::ParserError
        return
      end
      return unless arr.is_a?(Array) && arr[0].is_a?(String)

      name = arr[0]
      payload = arr[1]

      if name == 'CONSUME_CHUNK' && payload.is_a?(Hash)
        handle_consume_chunk(payload)
        return
      end

      deliver(name, payload)
    end

    def deliver(name, payload)
      handlers = @handlers_mutex.synchronize { @handlers[name].dup }
      handlers.each { |h| h.call(payload) }
    end

    def handle_consume_chunk(chunk)
      id = chunk['transferId']
      total = chunk['total'].to_i
      index = chunk['index'].to_i
      return if id.nil? || total <= 0 || index >= total

      transfer = (@chunks[id] ||= { 'parts' => Array.new(total),
                                    'event' => chunk['event'], 'received' => 0 })
      transfer['parts'][index] = chunk['data']
      transfer['received'] += 1
      return if transfer['received'] < total

      @chunks.delete(id)
      assembled = begin
        JSON.parse(transfer['parts'].join)
      rescue JSON::ParserError
        return
      end
      deliver(transfer['event'], assembled)
    end
  end
end
