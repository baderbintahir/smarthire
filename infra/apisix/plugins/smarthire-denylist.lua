-- Revocation at the edge (Phase 4.3, Appendix F.3).
--
-- Runs AFTER openid-connect (priority 2590 < 2599), so the token's signature, exp, iss and aud
-- are already verified when we get here; we only read its claims. NEVER attach this plugin to a
-- route without openid-connect: on its own it would trust an unverified token.
--
-- Checks the keys Identity writes in Redis (same keys the chassis verify_token reads):
--   denylist:{jti}            one token revoked by an admin
--   revoked_before:{user_id}  user suspended / "logout everywhere": tokens with iat <= value die
-- Redis unreachable -> 503 (fail CLOSED): never let an unchecked token through.
--
-- Also exposes the verified user id as the variable `smarthire_user_id`, so limit-count can
-- rate-limit per user.
local core = require("apisix.core")
local redis_new = require("resty.redis").new
local ngx = ngx
local tonumber = tonumber

local plugin_name = "smarthire-denylist"

local schema = {
    type = "object",
    properties = {
        redis_host = { type = "string", default = "redis" },
        redis_port = { type = "integer", default = 6379 },
        redis_timeout = { type = "integer", default = 1000 },  -- milliseconds
    },
}

local _M = {
    version = 0.1,
    priority = 2590,  -- access phase: after openid-connect (2599), before limit-count (1002)
    name = plugin_name,
    schema = schema,
}

core.ctx.register_var("smarthire_user_id", function(ctx)
    return ctx.smarthire_user_id
end)

local INVALID = { error = { code = "INVALID_TOKEN", message = "Missing, invalid or expired token" } }
local REVOKED = { error = { code = "UNAUTHORIZED", message = "Token has been revoked" } }
local UNAVAILABLE = {
    error = { code = "SERVICE_UNAVAILABLE", message = "Token verification is temporarily unavailable" },
}

function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end

-- Reads the claims of the (already verified) bearer token: the middle, base64url-encoded part.
local function token_claims(ctx)
    local header = core.request.header(ctx, "Authorization")
    local token = header and header:match("^[Bb]earer%s+(%S+)$")
    local payload = token and token:match("^[^.]+%.([^.]+)%.")
    if not payload then
        return nil
    end
    payload = payload:gsub("%-", "+"):gsub("_", "/")  -- base64url -> standard base64 ("%-": "-" is special in Lua patterns)
    local padding = #payload % 4
    if padding > 0 then
        payload = payload .. string.rep("=", 4 - padding)
    end
    local json = ngx.decode_base64(payload)
    return json and core.json.decode(json)
end

function _M.access(conf, ctx)
    local claims = token_claims(ctx)
    if not claims or not claims.jti or not claims.sub or not claims.iat then
        return 401, INVALID
    end

    local red = redis_new()
    red:set_timeouts(conf.redis_timeout, conf.redis_timeout, conf.redis_timeout)
    local ok, err = red:connect(conf.redis_host, conf.redis_port)
    if not ok then
        core.log.error("smarthire-denylist: redis connect failed: ", err)
        return 503, UNAVAILABLE
    end
    -- One round trip for both keys
    local res, merr = red:mget("denylist:" .. claims.jti, "revoked_before:" .. claims.sub)
    if not res then
        core.log.error("smarthire-denylist: redis MGET failed: ", merr)
        return 503, UNAVAILABLE
    end
    red:set_keepalive(10000, 100)  -- return the connection to the pool for the next request

    local denied = res[1] ~= ngx.null
    local revoked_before = res[2] ~= ngx.null and tonumber(res[2]) or nil
    -- "<=", same as the chassis: iat has 1-second resolution, so a token issued in the same
    -- second as the revocation can't be told apart from one issued just before it: reject both
    if denied or (revoked_before and claims.iat <= revoked_before) then
        return 401, REVOKED
    end

    ctx.smarthire_user_id = claims.sub
end

return _M
