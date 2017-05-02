local keyName = KEYS[1]
local requestedTokens = tonumber(ARGV[1])
local maxTokens = tonumber(ARGV[2])
local refillRatePerMs = tonumber(ARGV[3])
local currentTimeMs = tonumber(ARGV[4])

local log_level = redis.LOG_DEBUG
-- todo: remove log level statements, but they are useful for debugging at the moment
redis.log(log_level, "requestedTokens = " .. requestedTokens)
redis.log(log_level, "maxTokens = " .. maxTokens)
redis.log(log_level, "refillRatePerMs = " .. refillRatePerMs)
redis.log(log_level, "currentTimeMs = " .. currentTimeMs)

-- utility to get multiple kv pairs
local hmget = function (key, ...)
    if next(arg) == nil then return {} end
    local bulk = redis.call('HMGET', key, unpack(arg))
    local result = {}
    for i, v in ipairs(bulk) do result[ arg[i] ] = v end
    return result
end

-- load current data or set defaults
local getCurrentData = function(keyName, maxTokens, currentTimeMillis)
    local data = hmget(keyName, 'availableTokens', 'lastRefillMs')

    local availableTokens = maxTokens
    if data['availableTokens'] then
        availableTokens = tonumber(data['availableTokens'])
    end

    local lastRefillMs = currentTimeMillis
    if data['lastRefillMs'] then
        lastRefillMs = tonumber(data['lastRefillMs'])
    end

    --redis.log(log_level, "getCurrentData.available =  " .. availableTokens .. " , lastRefillMs =  " .. lastRefillMs)
    return availableTokens, lastRefillMs
end

-- calculate the integer amount to refill to
local amountToRefill = function(lastRefillMs, refillRatePerMs, currentTimeMillis)
    local elapsedMs = (currentTimeMillis - lastRefillMs)
    local refillAmount = elapsedMs * refillRatePerMs
    local refillAmountInt = math.floor(refillAmount)
    redis.log(log_level, "amountToRefill: elapsed = " .. elapsedMs .. " refillAmount = " .. refillAmount .. " refillAmountInt = " .. refillAmountInt)
    return refillAmountInt
end

-- number of milliseconds it will take to make the number of requests
local wholeRequestsInMs = function(refillRatePerMs, requestCount)
    return math.floor(1 / refillRatePerMs) * requestCount

end
-- update our entry
local update = function(keyName, nowAvailable, currentTimeMillis)
    redis.call('HMSET', keyName, 'availableTokens', nowAvailable, 'lastRefillMs', currentTimeMillis)
end

local expire = function(keyName, refillRatePerMs, maxTokens)
    local expireTime = math.max(1, wholeRequestsInMs(refillRatePerMs, maxTokens) * 2 / 1000)
    redis.log(log_level, "expiring: " .. expireTime)
    redis.call('EXPIRE', keyName, expireTime)
end

-- main logic
local availableTokens, lastRefillMs = getCurrentData(keyName, maxTokens, currentTimeMs)
local refillAmount = amountToRefill(lastRefillMs, refillRatePerMs, currentTimeMs)
local nowAvailable = math.min(maxTokens, availableTokens + refillAmount)
redis.log(log_level, "main.nowAvailable = " .. nowAvailable)
local refillToTime = math.floor(lastRefillMs + wholeRequestsInMs(refillRatePerMs, refillAmount))
redis.log(log_level, "main.refillToTime = " .. refillToTime)
local afterRequest = nowAvailable - requestedTokens
redis.log(log_level, "main.afterRequest = " .. afterRequest)

if afterRequest >= 0 then
    update(keyName, nowAvailable - requestedTokens, refillToTime);
    expire(keyName, refillRatePerMs, maxTokens)
    return requestedTokens
else
    if refillAmount > 0 then
        update(keyName, nowAvailable, refillToTime)
        expire(keyName, refillRatePerMs, maxTokens)
    end
    -- signal to the caller how many milliseconds to sleep for
    return wholeRequestsInMs(refillRatePerMs, afterRequest) + (currentTimeMs - refillToTime)
end
