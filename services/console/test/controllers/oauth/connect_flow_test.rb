require "test_helper"

module Oauth
  # The chat-principal consent flow: a signed connect link (Oauth::ConnectToken)
  # starts the ordinary consent flow without a console login, and the callback
  # grants the credential to the principal the link named, and to nobody else.
  # Exercised with the Merch Maker provider, a public client whose identity comes
  # from its userinfo endpoint.
  class ConnectFlowTest < ActionDispatch::IntegrationTest
    SECRET = "test-jwt-signing-secret".freeze

    setup do
      @app = OauthApp.create!(
        slug: "merchmaker", provider: "merchmaker", client_id: "assistant",
        allowed_scopes: %w[catalog:read catalog:write operator], enabled: true,
        created_by: users(:acme_admin)
      )
      @person = Principal.create!(
        foreign_id: "teams-user-person-a", kind: "teams_user", name: "Person A",
        created_by: users(:acme_admin)
      )
      @http_mocks = []
    end

    teardown do
      FlowsController.exchange_client_factory = -> { Broker::AuthorizationCodeClient.new }
      FlowsController.identity_http_client_factory = -> { HttpClient.new }
      @http_mocks.each(&:verify)
    end

    def connect_url(principal: @person, now: Time.current)
      token = with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) do
        ConnectToken.encode(app: @app, principal: principal, now: now)
      end
      oauth_connect_url(slug: @app.slug, t: token)
    end

    def open_link(url)
      with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) { get url }
    end

    def stub_merchmaker(sub:, tenant_id: "tenant-1", email: "person@example.test")
      exchange = expect_http_call(status: 200, body: {
        access_token: "AT-#{sub}", refresh_token: "RT-#{sub}", expires_in: 900,
        token_type: "bearer", scope: "catalog:read catalog:write operator"
      }.to_json) do |request|
        assert_equal Providers::Merchmaker::TOKEN_ENDPOINT, request[:url]
        assert_equal "assistant", request[:form]["client_id"]
        refute request[:form].key?("client_secret"), "a public client must not send a secret"
        assert request[:form]["code_verifier"].present?
      end
      identity = expect_http_call(status: 200, body: {
        sub: sub, email: email, email_verified: true, name: email,
        tenant_id: tenant_id, tenant_slug: "kept", tenant_name: "Kept", connection_kind: "OPERATOR"
      }.to_json) do |request|
        assert_equal Providers::Merchmaker::USERINFO_ENDPOINT, request[:url]
        assert_equal "Bearer AT-#{sub}", request[:headers]["Authorization"]
      end
      @http_mocks << exchange << identity
      FlowsController.exchange_client_factory = -> { Broker::AuthorizationCodeClient.new(http: exchange) }
      FlowsController.identity_http_client_factory = -> { HttpClient.new(http: identity) }
    end

    def complete_flow(url)
      open_link(url)
      assert_response :redirect
      uri = URI.parse(response.location)
      assert_equal "merchmaker.ai", uri.host
      query = URI.decode_www_form(uri.query).to_h
      assert_equal "https://merchmaker.ai/mcp", query["resource"]
      assert_equal "S256", query["code_challenge_method"]
      get oauth_callback_url(slug: @app.slug), params: { code: "code-1", state: query.fetch("state") }
    end

    def merchmaker_grants(principal)
      principal.grants.joins(static_secret: :broker_credential)
        .where(broker_credentials: { oauth_app_id: @app.id })
    end

    test "a merchmaker app needs no client secret" do
      assert @app.valid?
      assert_nil @app.client_secret
    end

    test "a connect link signs the person's account in without a console login and grants it to them alone" do
      stub_merchmaker(sub: "user-1")
      complete_flow(connect_url)

      assert_response :ok
      assert_match "connected as person@example.test", response.body
      credential = BrokerCredential.find_by!(oauth_app: @app)
      assert_equal "user-1.tenant-1", credential.provider_subject
      assert_equal "kept", credential.labels["merchmaker_tenant"]
      assert_equal users(:acme_admin), credential.created_by

      grants = merchmaker_grants(@person)
      assert_equal 1, grants.count
      assert_equal credential, grants.first.static_secret.broker_credential
      assert_equal 1, Grant.where(static_secret: grants.first.static_secret).count, "granted to anyone else"
    end

    test "connecting another account replaces the person's earlier grant" do
      stub_merchmaker(sub: "user-1")
      complete_flow(connect_url)
      stub_merchmaker(sub: "user-2", email: "other@example.test")
      complete_flow(connect_url)

      grants = merchmaker_grants(@person)
      assert_equal 1, grants.count
      assert_equal "user-2.tenant-1", grants.first.static_secret.broker_credential.provider_subject
    end

    test "an expired, forged or other-app link is refused" do
      open_link(connect_url(now: 11.minutes.ago))
      assert_response :bad_request

      forged = with_env("CENTAUR_JWT_SIGNING_SECRET" => "another-secret") do
        ConnectToken.encode(app: @app, principal: @person)
      end
      open_link(oauth_connect_url(slug: @app.slug, t: forged))
      assert_response :bad_request

      other = oauth_apps(:acme_google)
      token = with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) { ConnectToken.encode(app: other, principal: @person) }
      open_link(oauth_connect_url(slug: @app.slug, t: token))
      assert_response :bad_request
    end

    test "a shared principal cannot be issued a link" do
      assert_raises(ArgumentError) do
        with_env("CENTAUR_JWT_SIGNING_SECRET" => SECRET) do
          ConnectToken.encode(app: @app, principal: principals(:acme_channel))
        end
      end
    end

    test "the ordinary callback still requires a console login" do
      get oauth_callback_url(slug: @app.slug), params: { code: "code-1", state: "not-a-state" }
      assert_redirected_to login_path
    end
  end
end
