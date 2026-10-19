-- Adds the standard Retry-After header to 429 responses (Phase 4.4).
-- limit-count only sends X-RateLimit-Reset (seconds until the window resets); clients and
-- HTTP libraries look for Retry-After, so we copy the value across. Enabled globally.
local core = require("apisix.core")
local ngx = ngx
local math = math
local tonumber = tonumber

local plugin_name = "smarthire-retry-after"
local schema = { type = "object", properties = {} }

local _M = {
    version = 0.1,
    priority = 0,
    name = plugin_name,
    schema = schema,
}

function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end

-- header_filter phase: response headers are on their way back to the client
function _M.header_filter(conf, ctx)
    local reset = tonumber(ngx.header["X-RateLimit-Reset"])  -- fractional, e.g. "59.96"
    if ngx.status == 429 and reset and not ngx.header["Retry-After"] then
        -- Retry-After must be whole seconds: round UP so clients never retry too early
        ngx.header["Retry-After"] = math.ceil(reset)
    end
end

return _M
