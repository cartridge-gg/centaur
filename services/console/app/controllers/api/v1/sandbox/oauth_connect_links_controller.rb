module Api
  module V1
    module Sandbox
      # POST /api/v1/sandbox/oauth_apps/:slug/connect_link
      #
      # Mints a ten-minute consent link for the principal this sandbox serves,
      # so an agent can answer "connect your account" with a link the person
      # opens themselves. The principal comes from the sandbox entitlement token
      # (SandboxBaseController), never from the request.
      class OauthConnectLinksController < Api::SandboxBaseController
        def create
          app = OauthApp.find_by(slug: params[:slug], enabled: true)
          return render_error(status: :not_found, message: "unknown or disabled integration") if app.nil?

          principal = current_proxy.principal
          unless Oauth::ConnectToken.connectable?(principal)
            return render_error(
              status: :forbidden,
              message: "connect links are only issued in a person's own direct conversation"
            )
          end

          now = Time.current
          token = Oauth::ConnectToken.encode(app: app, principal: principal, now: now)
          render json: {
            data: {
              url: URI.join(public_base_url, "/oauth/#{app.slug}/connect?#{URI.encode_www_form(t: token)}").to_s,
              expires_at: (now + Oauth::ConnectToken::TTL).iso8601,
              connected: connected?(app, principal)
            }
          }
        end

        private

        # Whether this principal already holds a live grant for the app, so the
        # agent can say "reconnect" rather than "connect".
        def connected?(app, principal)
          principal.grants.joins(static_secret: :broker_credential)
            .where(broker_credentials: { oauth_app_id: app.id, dead: false }).exists?
        end

        def public_base_url
          ConsoleEnv["PUBLIC_URL"].presence || request.base_url
        end
      end
    end
  end
end
