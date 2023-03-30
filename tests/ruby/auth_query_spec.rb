# frozen_string_literal: true

require_relative 'spec_helper'
require_relative 'helpers/auth_query_helper'

describe "Auth Query" do
  let(:configured_instances) {[5432, 10432]}
  let(:config_user) { { 'username' => 'sharding_user', 'password' => 'sharding_user' } }
  let(:pg_user) { { 'username' => 'sharding_user', 'password' => 'sharding_user' } }
  let(:processes) { Helpers::AuthQuery.single_shard_auth_query(pool_name: "sharded_db", pg_user: pg_user, config_user: config_user, extra_conf: config, wait_until_ready: wait_until_ready ) }
  let(:config) { {} }
  let(:wait_until_ready) { true }

  after do
    unless @failing_process
      processes.all_databases.map(&:reset)
      processes.pgcat.shutdown
    end
    @failing_process = false
  end

  context "when auth_query is not configured" do
    context 'and cleartext passwords are set' do
      it "uses local passwords" do
        conn = PG.connect(processes.pgcat.connection_string("sharded_db", config_user['username'], config_user['password']))

        expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
      end
    end

    context 'and cleartext passwords are not set' do
      let(:config_user) { { 'username' => 'sharding_user' } }

      it "does not start because it is not possible to authenticate" do
        @failing_process = true
        expect { processes.pgcat }.to raise_error(StandardError, /You have to specify a user password for every pool if auth_query is not specified/)
      end
    end
  end

  context 'when auth_query is configured' do
    context 'with global configuration' do
      around(:example) do |example|

        # Set up auth query
        Helpers::AuthQuery.set_up_auth_query_for_user(
          user: 'md5_auth_user',
          password: 'secret'
        );

        example.run

        # Drop auth query support
        Helpers::AuthQuery.tear_down_auth_query_for_user(
          user: 'md5_auth_user',
          password: 'secret'
        );
      end

      context 'with correct global parameters' do
        let(:config) { { 'general' => { 'auth_query' => "SELECT * FROM public.user_lookup('$1');", 'auth_query_user' => 'md5_auth_user', 'auth_query_password' => 'secret' } } }
        context 'and with cleartext passwords set' do
          it 'it uses local passwords' do
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username'], pg_user['password']))
            expect(conn.exec("SELECT 1 + 2")).not_to be_nil
          end
        end

        context 'and with cleartext passwords not set' do
          let(:config_user) { { 'username' => 'sharding_user', 'password' => 'sharding_user' } }

          it 'it uses obtained passwords' do
            connection_string = processes.pgcat.connection_string("sharded_db", pg_user['username'], pg_user['password'])
            conn = PG.connect(connection_string)
            expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
          end

          it 'allows passwords to be changed without closing existing connections' do
            pgconn = PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username']))
            expect(pgconn.exec("SELECT 1 + 2")).not_to be_nil
            Helpers::AuthQuery.exec_in_instances(query: "ALTER USER #{pg_user['username']} WITH ENCRYPTED PASSWORD 'secret2';")
            expect(pgconn.exec("SELECT 1 + 4")).not_to be_nil
            Helpers::AuthQuery.exec_in_instances(query: "ALTER USER #{pg_user['username']} WITH ENCRYPTED PASSWORD '#{pg_user['password']}';")
          end

          it 'allows passwords to be changed and that new password is needed when reconnecting' do
            pgconn = PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username']))
            expect(pgconn.exec("SELECT 1 + 2")).not_to be_nil
            Helpers::AuthQuery.exec_in_instances(query: "ALTER USER #{pg_user['username']} WITH ENCRYPTED PASSWORD 'secret2';")
            newconn = PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username'], 'secret2'))
            expect(newconn.exec("SELECT 1 + 2")).not_to be_nil
            Helpers::AuthQuery.exec_in_instances(query: "ALTER USER #{pg_user['username']} WITH ENCRYPTED PASSWORD '#{pg_user['password']}';")
          end
        end
      end

      context 'with wrong parameters' do
        let(:config) { { 'general' => { 'auth_query' => 'SELECT 1', 'auth_query_user' => 'wrong_user', 'auth_query_password' => 'wrong' } } }

        context 'and with clear text passwords set' do
          it "it uses local passwords" do
            conn = PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username'], pg_user['password']))

            expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
          end
        end

        context 'and with cleartext passwords not set' do
          let(:config_user) { { 'username' => 'sharding_user' } }
          it "it fails to start as it cannot authenticate against servers" do
            @failing_process = true
            expect { PG.connect(processes.pgcat.connection_string("sharded_db", pg_user['username'], pg_user['password'])) }.to raise_error(StandardError, /Error trying to obtain password from auth_query/ )
          end

          context 'and we fix the issue and reload' do
            let(:wait_until_ready) { false }

            it 'fails in the beginning but starts working after reloading config' do
              connection_string = processes.pgcat.connection_string("sharded_db", pg_user['username'], pg_user['password'])
              while !(processes.pgcat.logs =~ /Waiting for clients/) do
                sleep 0.5
              end

              expect { PG.connect(connection_string)}.to raise_error(PG::ConnectionBad)
              expect(processes.pgcat.logs).to match(/Error trying to obtain password from auth_query/)

              current_config = processes.pgcat.current_config
              config = { 'general' => { 'auth_query' => "SELECT * FROM public.user_lookup('$1');", 'auth_query_user' => 'md5_auth_user', 'auth_query_password' => 'secret' } }
              processes.pgcat.update_config(current_config.deep_merge(config))
              processes.pgcat.reload_config

              conn = nil
              expect { conn = PG.connect(connection_string)}.not_to raise_error
              expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
            end
          end
        end
      end
    end

    context 'with per pool configuration' do
      around(:example) do |example|

        # Set up auth query
        Helpers::AuthQuery.set_up_auth_query_for_user(
          user: 'md5_auth_user',
          password: 'secret'
        );

        Helpers::AuthQuery.set_up_auth_query_for_user(
          user: 'md5_auth_user1',
          password: 'secret',
          database: 'shard1'
        );

        example.run

        # Tear down auth query
        Helpers::AuthQuery.tear_down_auth_query_for_user(
          user: 'md5_auth_user',
          password: 'secret'
        );

        Helpers::AuthQuery.tear_down_auth_query_for_user(
          user: 'md5_auth_user1',
          password: 'secret',
          database: 'shard1'
        );
      end

      context 'with correct parameters' do
        let(:processes) { Helpers::AuthQuery.two_pools_auth_query(pool_names: ["sharded_db0", "sharded_db1"], pg_user: pg_user, config_user: config_user, extra_conf: config ) }
        let(:config) {
          { 'pools' =>
            {
              'sharded_db0' => {
                'auth_query' => "SELECT * FROM public.user_lookup('$1');",
                'auth_query_user' => 'md5_auth_user',
                'auth_query_password' => 'secret'
              },
              'sharded_db1' => {
                'auth_query' => "SELECT * FROM public.user_lookup('$1');",
                'auth_query_user' => 'md5_auth_user1',
                'auth_query_password' => 'secret'
              },
            }
          }
        } 

        context 'and with cleartext passwords set' do
          it 'it uses local passwords' do
            conn = PG.connect(processes.pgcat.connection_string("sharded_db0", pg_user['username'], pg_user['password']))
            expect(conn.exec("SELECT 1 + 2")).not_to be_nil
            conn = PG.connect(processes.pgcat.connection_string("sharded_db1", pg_user['username'], pg_user['password']))
            expect(conn.exec("SELECT 1 + 2")).not_to be_nil
          end
        end

        context 'and with cleartext passwords not set' do
          let(:config_user) { { 'username' => 'sharding_user' } }

          it 'it uses obtained passwords' do
            connection_string = processes.pgcat.connection_string("sharded_db0", pg_user['username'], pg_user['password'])
            conn = PG.connect(connection_string)
            expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
            connection_string = processes.pgcat.connection_string("sharded_db1", pg_user['username'], pg_user['password'])
            conn = PG.connect(connection_string)
            expect(conn.async_exec("SELECT 1 + 2")).not_to be_nil
          end
        end

      end
    end
  end
end
