module Oauth
  # A short-lived, signed link that lets ONE chat principal connect its own
  # account for ONE OAuth app without a console login.
  #
  # Chat users (a Microsoft Teams person in a direct message, say) have no
  # console account, so the ordinary consent flow, which needs a console
  # session, is closed to them. The sandbox serving that person's own principal
  # asks the console for this link (it cannot name any other principal: the
  # console reads the principal from the sandbox's entitlement token), the agent
  # replies with it, and the consent flow it starts grants the resulting
  # credential to that principal alone.
  #
  # The link is a bearer capability for ten minutes: whoever opens it first can
  # attach their account to that principal. It is only minted for principals
  # that belong to exactly one person (CONNECTABLE_PRINCIPAL_KINDS), it is
  # posted only in that person's direct conversation, and the callback page and
  # the agent both say which account was connected.
  module ConnectToken
    AUDIENCE = "centaur-console-oauth-connect".freeze
    ISSUER = "centaur-console".freeze
    TTL = 10.minutes
    # Principals that are one person. A shared conversation or channel
    # principal is never connectable: its credential would act for everyone in
    # it.
    CONNECTABLE_PRINCIPAL_KINDS = %w[teams_user].freeze

    InvalidToken = Class.new(StandardError)

    module_function

    def connectable?(principal)
      principal.present? && CONNECTABLE_PRINCIPAL_KINDS.include?(principal.kind)
    end

    def encode(app:, principal:, now: Time.current)
      raise ArgumentError, "principal #{principal&.oid} cannot connect accounts" unless connectable?(principal)

      CentaurJwt::Hs256.encode(
        {
          "aud" => AUDIENCE, "iss" => ISSUER,
          "iat" => now.to_i, "exp" => (now + TTL).to_i,
          "app" => app.oid, "principal" => principal.oid,
          "jti" => SecureRandom.urlsafe_base64(16)
        },
        signing_secret: signing_secret
      )
    end

    # Returns the claims, or raises InvalidToken for anything expired, forged,
    # or minted for another app.
    def decode(token, app:)
      claims = CentaurJwt::Hs256.decode(token, signing_secret: signing_secret, aud: AUDIENCE, iss: ISSUER)
      raise InvalidToken, "connect link is for another integration" unless claims["app"] == app.oid

      claims
    rescue CentaurJwt::Hs256::VerificationError, KeyError => e
      raise InvalidToken, e.message
    end

    def signing_secret = ENV["CENTAUR_JWT_SIGNING_SECRET"].to_s
  end
end
