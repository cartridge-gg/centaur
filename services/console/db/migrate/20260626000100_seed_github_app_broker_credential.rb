# frozen_string_literal: true

# Provision the GitHub App broker credential from the deployment's GITHUB_APP_*
# env (the same values the old inline `github_app` secret source read) so the
# `github_auth_headers` tool's `token_broker` source can resolve it and the
# broker mints/rotates the installation token out-of-band.
#
# Idempotent (find_or_create by namespace+foreign_id) and a no-op where the env
# is unset (dev/test). Uses the model so client_secret is stored encrypted; a
# failure is logged rather than raised so it never breaks a console deploy.
class SeedGithubAppBrokerCredential < ActiveRecord::Migration[8.1]
  def up
    app_id = ENV["GITHUB_APP_ID"].to_s
    installation_id = ENV["GITHUB_APP_INSTALLATION_ID"].to_s
    private_key_b64 = ENV["GITHUB_APP_PRIVATE_KEY_B64"].to_s
    if app_id.blank? || installation_id.blank? || private_key_b64.blank?
      say "GITHUB_APP_* env not set; skipping github_app broker credential seed"
      return
    end

    BrokerCredential.find_or_create_by!(namespace: "default", foreign_id: "github-app") do |c|
      c.grant = "github_app"
      c.client_id = app_id
      c.token_endpoint = "https://api.github.com/app/installations/#{installation_id}/access_tokens"
      c.client_secret = private_key_b64
    end
    say "seeded github_app broker credential (namespace=default foreign_id=github-app)"
  rescue StandardError => e
    say "WARNING: github_app broker credential seed failed: #{e.class}: #{e.message}"
  end

  def down
    BrokerCredential.where(namespace: "default", foreign_id: "github-app", grant: "github_app").destroy_all
  end
end
