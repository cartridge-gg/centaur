require "test_helper"

module Api
  module V1
    class SandboxOauthConnectLinksControllerTest < ActionDispatch::IntegrationTest
      SECRET = "test-secret".freeze

      setup do
        @app = OauthApp.create!(
          slug: "merchmaker", provider: "merchmaker", client_id: "assistant",
          allowed_scopes: %w[catalog:read], enabled: true, created_by: users(:acme_admin)
        )
        @person = Principal.create!(
          foreign_id: "teams-user-person-a", kind: "teams_user", name: "Person A",
          created_by: users(:acme_admin)
        )
        @proxy = Proxy.create!(name: "person-a-sandbox", principal: @person,
                               bearer_token_hash: Digest::SHA256.hexdigest("iprx_#{'c' * 64}"))
      end

      test "mints a link for the sandbox's own person" do
        with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET, "CENTAUR_CONSOLE_PUBLIC_URL" => "https://console.example.test") do
          post "/api/v1/sandbox/oauth_apps/merchmaker/connect_link", headers: auth_headers(@proxy)
          assert_response :ok

          data = JSON.parse(response.body).fetch("data")
          uri = URI.parse(data.fetch("url"))
          assert_equal "console.example.test", uri.host
          assert_equal "/oauth/merchmaker/connect", uri.path
          claims = Oauth::ConnectToken.decode(URI.decode_www_form(uri.query).to_h.fetch("t"), app: @app)
          assert_equal @person.oid, claims["principal"]
          assert_equal false, data.fetch("connected")
        end
      end

      test "refuses a shared conversation's sandbox" do
        with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) do
          post "/api/v1/sandbox/oauth_apps/merchmaker/connect_link", headers: auth_headers(proxies(:acme_proxy))
        end
        assert_response :forbidden
      end

      test "refuses an unknown or disabled app" do
        @app.update!(enabled: false)
        with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) do
          post "/api/v1/sandbox/oauth_apps/merchmaker/connect_link", headers: auth_headers(@proxy)
        end
        assert_response :not_found
      end

      test "refuses a request without a sandbox token" do
        post "/api/v1/sandbox/oauth_apps/merchmaker/connect_link"
        assert_response :unauthorized
      end

      private

      def auth_headers(proxy)
        { "Authorization" => "Bearer #{SandboxEntitlements::Jwt.encode_for_proxy(proxy)}" }
      end
    end
  end
end
