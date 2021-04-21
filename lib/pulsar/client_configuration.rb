#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'pulsar/bindings'

module Pulsar
  class ClientConfiguration
    AUTH_TOKEN_PLUGIN_NAME = 'org.apache.pulsar.client.impl.auth.AuthenticationToken'.freeze
    CLIENT_CONF_KEY_VALUE_SEPARATOR = ': '
    FILE_URL_PREFIX = 'file://'.freeze

    def self.from(config)
      case config
      when self then config # already a config object
      when nil then self.new # empty (all defaults) config object
      when Hash then self.new(config) # config object from hash
      else raise ArgumentError
      end
    end

    # Creates a client configuration from a custom and environment configuration
    # (the environment configuration defaults to OS's ENV)
    def self.from_environment(config={}, environment=ENV.to_h)
      environment_config = {}
      if environment.has_key?('PULSAR_CERT_PATH')
        environment_config[:use_tls] = true
        environment_config[:tls_allow_insecure_connection] = false
        environment_config[:tls_validate_hostname] = false
        environment_config[:tls_trust_certs_file_path] = environment['PULSAR_CERT_PATH']
      end
      if environment.has_key?('PULSAR_AUTH_TOKEN')
        environment_config[:authentication_token] = environment['PULSAR_AUTH_TOKEN']
      end
      if environment.has_key?('PULSAR_CLIENT_CONF')
        environment_config.merge!(read_from_client_conf(environment['PULSAR_CLIENT_CONF']))
      end
      if environment.has_key?('PULSAR_AUTH_OAUTH2_PARAMS')
        environment_config[:authentication_oauth2_params] = environment['PULSAR_AUTH_OAUTH2_PARAMS']
      end
      self.from(environment_config.merge(config))
    end

    # Load configuration from PULSAR_CLIENT_CONF
    # refer to (see https://github.com/apache/pulsar/blob/master/conf/client.conf)
    def self.read_from_client_conf(pulsar_client_conf_file)
      client_config = {}
      # Read the client config as a Ruby Hashmap
      pulsar_config = read_config(pulsar_client_conf_file)
      # Attempt to read as many configuration as possible from the client configuration

      # If a TLS certificate had been given, use it
      if pulsar_config.has_key? 'tlsTrustCertsFilePath'
        client_config[:use_tls] = true
        client_config[:tls_trust_certs_file_path] = pulsar_config['tlsTrustCertsFilePath']
      end
      # If 'TLS enable hostname verification' is false, then switch it off in config
      if (pulsar_config.has_key? 'tlsEnableHostnameVerification') &&
          pulsar_config['tlsEnableHostnameVerification'].eql?('false')
        client_config[:tls_validate_hostname] = false
      end
      # If 'TLS allow insecure connection' is false, then switch it off in config
      if (pulsar_config.has_key? 'tlsAllowInsecureConnection') &&
          pulsar_config['tlsAllowInsecureConnection'].eql?('false')
        client_config[:tls_allow_insecure_connection] = false
      end
      # If token-based authentication is used, load the token
      # Currently only loading from files is supported
      if (pulsar_config['authPlugin'].eql? AUTH_TOKEN_PLUGIN_NAME) &&
          pulsar_config['authParams'].start_with?(FILE_URL_PREFIX)
        # read the first line from the token file
        token_file = pulsar_config['authParams'].gsub(FILE_URL_PREFIX, '')
        begin
          token_file_lines = File.readlines(token_file)
          # store the token to the config
          client_config[:authentication_token] = token_file_lines[0]
        rescue Errno::ENOENT => e
          warn("Could not load token file '#{token_file}'. Exception was #{e}.")
        end
      end
      # If we have a broker service URI, use it
      client_config[:broker_uri] = pulsar_config['brokerServiceUrl'] if pulsar_config.has_key? 'brokerServiceUrl'
      client_config
    end

    # read Pulsar client configuration as a Hashmap from a client configuration file
    def self.read_config(pulsar_config_file)
      result = []
      begin
        config_file_content = File.readlines(pulsar_config_file)
        result = config_file_content.map do |line|
          key_value = line.split(CLIENT_CONF_KEY_VALUE_SEPARATOR, 2)
          [key_value[0], key_value[1].strip]
        end
      rescue Errno::ENOENT => e
        warn("Could not load client config file '#{pulsar_config_file}'. Exception was #{e}.")
      end
      return result.to_h
    end

    module RubySideTweaks
      def initialize(config={})
        super()
        # store client configuration inside class
        @config = config
        populate(config)
      end
    end

    prepend RubySideTweaks

    # serve the client configuration to peer objects
    # (eg. we need to configure the 'broker URI' in a different way)
    def [](key)
      @config[key]
    end

    def populate(config={})
      populate_one(config, :authentication_token)
      populate_one(config, :authentication_oauth2_params)
      populate_one(config, :operation_timeout_seconds)
      populate_one(config, :io_threads)
      populate_one(config, :message_listener_threads)
      populate_one(config, :concurrent_lookup_requests)
      populate_one(config, :log_conf_file_path)
      populate_one(config, :silent_logging)
      populate_one(config, :use_tls)
      populate_one(config, :tls_trust_certs_file_path)
      populate_one(config, :tls_allow_insecure_connection)
      populate_one(config, :tls_validate_hostname)
    end

    def populate_one(config, key)
      if config.key?(key)
        self.send(:"#{key}=", config[key])
      elsif config.key?(key.to_s)
        self.send(:"#{key}=", config[key.to_s])
      end
    end
  end
end
