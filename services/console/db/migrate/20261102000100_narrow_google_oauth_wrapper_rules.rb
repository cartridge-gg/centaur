# Narrows the request rules of existing Google OAuth wrapper secrets from
# `*.googleapis.com` to the Workspace API hosts.
#
# The consent flow gave each Google wrapper secret one rule for
# `*.googleapis.com`. That rule also matches the Google Cloud API hosts
# (logging, monitoring, connectgateway, ...). A principal that held a Google
# login and a gcp_auth secret then sent its Workspace-scoped token to Cloud APIs
# and got 403 ACCESS_TOKEN_SCOPE_INSUFFICIENT. New wrappers get the narrow list
# from Oauth::Providers::Google::API_HOSTS; this migration moves existing ones.
#
# Only a wrapper that still has the untouched default changes: exactly one rule,
# host `*.googleapis.com`, no cidr, no methods, no paths. A wrapper an operator
# edited keeps its rules, as on re-consent. Only rule rows change. The secret,
# its token_broker source and its grants stay as they are. Every principal that
# holds a changed secret, directly or through a role, gets a sync-config cache
# bump, so its proxy picks up the new rules on the next sync.
#
# The host list is frozen here, not read from API_HOSTS, so a later change to the
# constant does not change what this migration did.
class NarrowGoogleOauthWrapperRules < ActiveRecord::Migration[8.1]
  OLD_HOST = "*.googleapis.com".freeze
  NEW_HOSTS = %w[
    gmail.googleapis.com
    calendar.googleapis.com
    people.googleapis.com
    www.googleapis.com
    drive.googleapis.com
    docs.googleapis.com
    sheets.googleapis.com
    slides.googleapis.com
    analyticsdata.googleapis.com
    docs.google.com
  ].freeze

  def up
    secret_ids = select_values(<<~SQL.squish).map(&:to_i)
      SELECT static_secrets.id
      FROM static_secrets
      INNER JOIN broker_credentials ON broker_credentials.id = static_secrets.broker_credential_id
      INNER JOIN oauth_apps ON oauth_apps.id = broker_credentials.oauth_app_id
      WHERE oauth_apps.provider = 'google'
        AND EXISTS (
          SELECT 1
          FROM request_rules
          WHERE request_rules.static_secret_id = static_secrets.id
            AND request_rules.host = #{connection.quote(OLD_HOST)}
            AND request_rules.cidr IS NULL
            AND request_rules.http_methods = '[]'::jsonb
            AND request_rules.paths = '[]'::jsonb
        )
        AND (
          SELECT count(*)
          FROM request_rules
          WHERE request_rules.static_secret_id = static_secrets.id
        ) = 1
    SQL
    if secret_ids.empty?
      say "no Google OAuth wrapper secret has the default *.googleapis.com rule"
      return
    end

    ids = secret_ids.join(", ")
    host_rows = NEW_HOSTS.each_with_index.map { |host, position| "(#{connection.quote(host)}, #{position})" }.join(", ")

    execute "DELETE FROM request_rules WHERE static_secret_id IN (#{ids})"
    execute <<~SQL.squish
      INSERT INTO request_rules (static_secret_id, host, http_methods, paths, position, created_at, updated_at)
      SELECT static_secrets.id, hosts.host, '[]'::jsonb, '[]'::jsonb, hosts.position,
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
      FROM static_secrets
      CROSS JOIN (VALUES #{host_rows}) AS hosts(host, position)
      WHERE static_secrets.id IN (#{ids})
    SQL
    execute "UPDATE static_secrets SET updated_at = CURRENT_TIMESTAMP WHERE id IN (#{ids})"
    execute <<~SQL.squish
      UPDATE principals
      SET sync_config_cache_version = sync_config_cache_version + 1,
          updated_at = CURRENT_TIMESTAMP
      WHERE principals.id IN (
        SELECT grants.principal_id
        FROM grants
        WHERE grants.static_secret_id IN (#{ids})
          AND grants.principal_id IS NOT NULL
        UNION
        SELECT principal_roles.principal_id
        FROM grants
        INNER JOIN principal_roles ON principal_roles.role_id = grants.role_id
        WHERE grants.static_secret_id IN (#{ids})
          AND grants.role_id IS NOT NULL
      )
    SQL
    say "narrowed #{secret_ids.size} Google OAuth wrapper secret(s) to #{NEW_HOSTS.size} Workspace hosts"
  end

  def down
    # One-way credential repair. The wildcard rule is the bug, and operators can
    # edit these rules after rollout, so a rollback must not restore it.
  end
end
