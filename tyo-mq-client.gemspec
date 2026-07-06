# frozen_string_literal: true

require_relative 'lib/tyo_mq_client/version'

Gem::Specification.new do |spec|
  spec.name = 'tyo-mq-client'
  spec.version = TyoMq::VERSION
  spec.authors = ['TYO Lab']
  spec.email = ['dev@tyo.com.au']

  spec.summary = 'Ruby client for tyo-mq'
  spec.description = 'Ruby client for tyo-mq: pub/sub messaging with durable ' \
                     'ACK/retry delivery, topic wildcards, consumer groups, ' \
                     'and auth realms'
  spec.homepage = 'https://github.com/tyolab/tyo-mq-client-ruby'
  spec.license = 'Apache-2.0'
  spec.required_ruby_version = '>= 2.7'

  spec.files = Dir['lib/**/*.rb'] + %w[README.md LICENSE]
  spec.require_paths = ['lib']

  spec.add_dependency 'websocket-client-simple', '~> 0.9'
end
