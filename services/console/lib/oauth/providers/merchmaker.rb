module Oauth
  module Providers
    # Merch Maker OAuth consent-flow strategy, for its remote MCP server
    # (https://merchmaker.ai/mcp). Merch Maker registers predefined PUBLIC
    # clients: PKCE S256, no client secret, and it refuses a secret if one is
    # sent. The token response carries no id_token, so identity comes from the
    # authorization server's userinfo endpoint, which re-reads the live
    # connection behind the token.
    #
    # A person may connect more than one Merch Maker workspace; each connection
    # is its own grant, so the subject is the account AND the workspace.
    class Merchmaker
      include HttpIdentity

      KEY = "merchmaker"
      ISSUER = "https://merchmaker.ai".freeze
      AUTHORIZATION_ENDPOINT = "#{ISSUER}/oauth/authorize".freeze
      TOKEN_ENDPOINT = "#{ISSUER}/oauth/token".freeze
      USERINFO_ENDPOINT = "#{ISSUER}/oauth/userinfo".freeze
      RESOURCE = "#{ISSUER}/mcp".freeze
      IDENTITY_SCOPES = [].freeze
      API_HOSTS = %w[merchmaker.ai].freeze

      def key = KEY
      def display_name = "Merch Maker"
      def authorization_endpoint = AUTHORIZATION_ENDPOINT
      def token_endpoint = TOKEN_ENDPOINT
      def identity_scopes = IDENTITY_SCOPES
      def api_hosts = API_HOSTS
      def authorization_scope_param = "scope"
      def scope_separator = " "
      # RFC 8707: the token is bound to the MCP resource.
      def extra_authorization_params = { "resource" => RESOURCE }
      def refreshable? = true
      # No client secret exists; client_secret_post with a blank secret sends
      # only client_id, which is what a public client must do.
      def public_client? = true
      def token_endpoint_auth_method = "client_secret_post"

      def parse_granted_scopes(scope) = scope.to_s.split
      def refresh_scopes(scopes) = Array(scopes)

      def identity_from(result, client_id:, http_client: HttpClient.new)
        if result.access_token.blank?
          raise Broker::ExchangeError.new("token response returned an empty access_token",
                                          stage: "parse", code: "missing_access_token")
        end

        response = identity_response(provider: display_name) do
          http_client.get(
            USERINFO_ENDPOINT,
            headers: { "Authorization" => "Bearer #{result.access_token}", "User-Agent" => "centaur-console" }
          )
        end
        info = identity_json(response, provider: display_name)
        account = require_identity(info["sub"], provider: display_name).to_s
        workspace = require_identity(info["tenant_id"], provider: display_name).to_s

        {
          subject: "#{account}.#{workspace}",
          email: info["email"].presence,
          name: [ info["email"].presence || account, info["tenant_name"].presence ].compact.join(" · "),
          labels: {
            "merchmaker_tenant" => info["tenant_slug"].to_s,
            "merchmaker_connection" => info["connection_kind"].to_s
          }.reject { |_, value| value.blank? }
        }
      end
    end
  end
end
