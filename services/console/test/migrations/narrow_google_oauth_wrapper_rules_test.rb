require "test_helper"
require Rails.root.join("db/migrate/20261102000100_narrow_google_oauth_wrapper_rules")

class NarrowGoogleOauthWrapperRulesTest < ActiveSupport::TestCase
  WILDCARD = NarrowGoogleOauthWrapperRules::OLD_HOST
  NEW_HOSTS = NarrowGoogleOauthWrapperRules::NEW_HOSTS

  # A wrapper secret as the consent flow creates it, with the given rules.
  def google_wrapper(rules: [ { host: WILDCARD } ], app: oauth_apps(:acme_google))
    credential = BrokerCredential.create!(
      foreign_id: "google-#{SecureRandom.hex(4)}",
      token_endpoint: "https://oauth2.googleapis.com/token",
      oauth_app: app,
      provider_subject: "sub-#{SecureRandom.hex(4)}"
    )
    secret = StaticSecret.new(
      name: "Google - user@example.com token",
      broker_credential: credential,
      inject_config: { "header" => "Authorization", "formatter" => "Bearer {{ .Value }}" }
    )
    secret.build_source(source_type: "token_broker", config: { "credential_id" => credential.oid })
    rules.each_with_index { |rule, position| secret.rules.build(rule.merge(position: position)) }
    secret.save!
    secret
  end

  def migrate
    ActiveRecord::Migration.suppress_messages { NarrowGoogleOauthWrapperRules.new.up }
  end

  def rule_rows(secret)
    secret.reload.rules.map { |rule| [ rule.host, rule.http_methods, rule.paths ] }
  end

  test "narrows the default wildcard to the Workspace hosts and keeps the token source" do
    secret = google_wrapper
    credential_oid = secret.broker_credential.oid

    migrate

    secret.reload
    assert_equal NEW_HOSTS, secret.rules.map(&:host)
    assert_equal (0...NEW_HOSTS.size).to_a, secret.rules.map(&:position)
    assert secret.rules.all? { |rule| rule.http_methods.empty? && rule.paths.empty? && rule.cidr.nil? }
    assert_equal "token_broker", secret.source.source_type
    assert_equal credential_oid, secret.source.config["credential_id"]
    assert_equal({ "header" => "Authorization", "formatter" => "Bearer {{ .Value }}" }, secret.inject_config)
  end

  test "bumps the sync-config cache of direct and role grantees once each" do
    secret = google_wrapper
    direct = principals(:acme_channel) # also a member of acme_infra
    role_member = principals(:acme_user_alice)
    Grant.create!(principal: direct, static_secret: secret, created_by: users(:acme_admin))
    Grant.create!(role: roles(:acme_infra), static_secret: secret, created_by: users(:acme_admin))
    direct_version = direct.reload.sync_config_cache_version
    role_member_version = role_member.reload.sync_config_cache_version

    migrate

    assert_equal direct_version + 1, direct.reload.sync_config_cache_version
    assert_equal role_member_version + 1, role_member.reload.sync_config_cache_version
  end

  test "leaves edited wrappers, non-Google secrets and gcp_auth secrets unchanged" do
    extra_rule = google_wrapper(rules: [ { host: WILDCARD }, { host: "youtube.googleapis.com" } ])
    narrowed_by_hand = google_wrapper(rules: [ { host: "gmail.googleapis.com" } ])
    method_limited = google_wrapper(rules: [ { host: WILDCARD, http_methods: [ "GET" ] } ])
    not_google = StaticSecret.create!(
      name: "operator Google API key",
      inject_config: { "header" => "x-goog-api-key" },
      created_by: users(:acme_admin)
    )
    RequestRule.create!(host: WILDCARD, static_secret: not_google)
    untouched = [ extra_rule, narrowed_by_hand, method_limited, not_google ].index_with { |s| rule_rows(s) }
    gcp_rules = gcp_auth_secrets.index_with { |s| s.rules.map(&:host) }

    migrate

    untouched.each { |secret, rows| assert_equal rows, rule_rows(secret), secret.name }
    gcp_rules.each { |secret, hosts| assert_equal hosts, secret.reload.rules.map(&:host) }
  end

  test "a second run changes nothing" do
    secret = google_wrapper
    migrate
    rule_ids = secret.reload.rules.map(&:id)
    principal = principals(:acme_channel)
    Grant.create!(principal: principal, static_secret: secret, created_by: users(:acme_admin))
    version = principal.reload.sync_config_cache_version

    migrate

    assert_equal rule_ids, secret.reload.rules.map(&:id)
    assert_equal version, principal.reload.sync_config_cache_version
  end
end
