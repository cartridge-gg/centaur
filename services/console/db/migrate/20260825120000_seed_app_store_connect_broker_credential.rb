# frozen_string_literal: true

# Provision a short-lived App Store Connect bearer-token broker from deployment
# env. The EC private key remains encrypted in Console and never reaches a
# session sandbox; iron-proxy only receives the broker's minted JWT.
class SeedAppStoreConnectBrokerCredential < ActiveRecord::Migration[8.1]
  def up
    issuer_id = ENV["ASC_ISSUER_ID"].to_s
    key_id = ENV["ASC_KEY_ID"].to_s
    private_key_b64 = ENV["ASC_PRIVATE_KEY_B64"].to_s
    if issuer_id.blank? || key_id.blank? || private_key_b64.blank?
      say "ASC_* env not set; skipping app_store_connect broker credential seed"
      return
    end

    BrokerCredential.find_or_create_by!(namespace: "default", foreign_id: "app-store-connect") do |c|
      c.grant = "app_store_connect"
      c.client_id = issuer_id
      c.external_user_key = key_id
      c.client_secret = private_key_b64
    end
    say "seeded app_store_connect broker credential (namespace=default foreign_id=app-store-connect)"
  rescue StandardError => e
    say "WARNING: app_store_connect broker credential seed failed: #{e.class}: #{e.message}"
  end

  def down
    BrokerCredential.where(
      namespace: "default",
      foreign_id: "app-store-connect",
      grant: "app_store_connect"
    ).destroy_all
  end
end
