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
require 'tempfile'

RSpec.describe Pulsar::ClientConfiguration do
  it 'use authentication token from environment' do
    test_env = {
      'PULSAR_AUTH_TOKEN' => 'example.token.abcdef123'
    }
    config = Pulsar::ClientConfiguration.from_environment({}, test_env)
    expect(config[:authentication_token]).to eq('example.token.abcdef123')
  end

  it 'use client cert from environment' do
    test_env = {
      'PULSAR_CERT_PATH' => '/path/to/cert.pem'
    }
    config = Pulsar::ClientConfiguration.from_environment({}, test_env)
    expect(config[:use_tls]).to eq(true)
    expect(config[:tls_allow_insecure_connection]).to eq(false)
    expect(config[:tls_validate_hostname]).to eq(false)
    expect(config[:tls_trust_certs_file_path]).to eq('/path/to/cert.pem')
  end

  it 'do not use authentication token when plugin is not set' do
    test_token = create_temp_config(
      'example.token.abcdef123'
    )
    test_config = create_temp_config(
      "authParams: file://#{test_token.path}"
    )
    begin
      test_env = {
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:authentication_token]).to eq(nil)
    ensure
      test_config.unlink
      test_token.unlink
    end
  end

  it 'overwrite token with client configuration' do
    test_token = create_temp_config(
      'example.token.abcdef123.from.file'
    )
    test_config = create_temp_config(
      "authPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken\n"\
      "authParams: file://#{test_token.path}"
    )
    begin
      test_env = {
        'PULSAR_AUTH_TOKEN' => 'example.token.abcdef123.from.env.shall.be.overwritten',
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:authentication_token]).to eq('example.token.abcdef123.from.file')
    ensure
      test_config.unlink
      test_token.unlink
    end
  end

  it 'overwrite certificate with client configuration' do
    test_config = create_temp_config(
      "tlsTrustCertsFilePath: /cert/file/from/client/config/ca.pem\n"
    )
    begin
      test_env = {
        'PULSAR_CERT_PATH' => '/path/to/a/cert/that/shall/not/be/used',
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:tls_trust_certs_file_path]).to eq('/cert/file/from/client/config/ca.pem')
    ensure
      test_config.unlink
    end
  end

  it 'load all supported configuration from configuration file' do
    test_token = create_temp_config(
      'example.token.abcdef123.from.file'
    )
    test_config = create_temp_config(
      "authPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken\n"\
      "authParams: file://#{test_token.path}\n"\
      "brokerServiceUrl: pulsar+ssl://test.broker.uri:6651\n"\
      "tlsTrustCertsFilePath: /test/cert/file/ca.pem\n"\
      "tlsAllowInsecureConnection: false\n"\
      "tlsEnableHostnameVerification: false\n"
    )
    begin
      test_env = {
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:authentication_token]).to eq('example.token.abcdef123.from.file')
      expect(config[:broker_uri]).to eq('pulsar+ssl://test.broker.uri:6651')
      expect(config[:tls_allow_insecure_connection]).to eq(false)
      expect(config[:tls_validate_hostname]).to eq(false)
      expect(config[:tls_trust_certs_file_path]).to eq('/test/cert/file/ca.pem')
      expect(config[:use_tls]).to eq(true)
    ensure
      test_config.unlink
      test_token.unlink
    end
  end

  it 'handle when configuration file is not found without exceptions' do
    test_env = {
      'PULSAR_CLIENT_CONF' => '/this/file/does/not/exists'
    }
    config = Pulsar::ClientConfiguration.from_environment({}, test_env)
    expect(config[:authentication_token]).to eq(nil)
    expect(config[:broker_uri]).to eq(nil)
    expect(config[:tls_allow_insecure_connection]).to eq(nil)
    expect(config[:tls_validate_hostname]).to eq(nil)
    expect(config[:tls_trust_certs_file_path]).to eq(nil)
    expect(config[:use_tls]).to eq(nil)
  end

  it 'handle when token file is not found without exceptions' do
    test_config = create_temp_config(
      "authPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken\n"\
      "authParams: file:///missing/token/file.txt"
    )
    begin
      test_env = {
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:authentication_token]).to eq(nil)
    ensure
      test_config.unlink
    end
  end

  it 'do not load auth params with incorrect prefix' do
    # the authParams has a missing schema in this config
    test_config = create_temp_config(
      'authPlugin: org.apache.pulsar.client.impl.auth.AuthenticationToken\n'\
      'authParams: /missing/token/file.txt'
    )
    begin
      test_env = {
        'PULSAR_CLIENT_CONF' => test_config.path
      }
      config = Pulsar::ClientConfiguration.from_environment({}, test_env)
      expect(config[:authentication_token]).to eq(nil)
    ensure
      test_config.unlink
    end
  end

  it 'use OAuth2 params from environment' do
    test_env = {
      'PULSAR_AUTH_OAUTH2_PARAMS' => '{"issuer_url":"https://some.issuer.url","audience":"urn:some:audience","private_key":"/path/to/credentials/file.json"}'
    }
    config = Pulsar::ClientConfiguration.from_environment({}, test_env)
    expect(config[:authentication_oauth2_params]).to eq('{"issuer_url":"https://some.issuer.url","audience":"urn:some:audience","private_key":"/path/to/credentials/file.json"}')
  end

  describe "silent logging" do
    it "is off by default" do
      expect(Pulsar::ClientConfiguration.new.silent_logging?).to eq(false)
    end

    it "can be enabled" do
      expect(Pulsar::ClientConfiguration.from({silent_logging: true}).silent_logging?).to eq(true)
    end

    it "raises when set twice" do
      expect {
        Pulsar::ClientConfiguration.from({silent_logging: true}).silent_logging = false
      }.to raise_error(ArgumentError, /silent_logging can only be set once/)
    end
  end
end

def create_temp_config(content)
  file = Tempfile.new('pulsar-ruby-client-configuration-test')
  file.write(content)
  file.close
  file
end
