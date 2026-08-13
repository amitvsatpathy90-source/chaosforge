-- Atomic sliding-window rate limiter (true global: one shared key per tenant, not per-pod).
-- KEYS[1] = {tenant:<id>}:rate      (hash-tagged for Redis Cluster co-location)
-- KEYS[2] = {tenant:<id>}:rate:seq  (member-uniqueness counter — DECLARED, not computed in-script)
-- ARGV[1] = window_ms
-- ARGV[2] = limit (max requests per window)
-- Returns: remaining quota (>= 0) when allowed; -1 when the limit is breached.
--
-- Both keys are passed via KEYS (arch-audit L2): a script must declare every key it touches so Redis can
-- verify same-slot co-location and (under Redis Functions / strict effect replication) not reject an
-- undeclared runtime key. The shared {tenant:<id>} hash tag keeps KEYS[1] and KEYS[2] in one slot.
--
-- CLOCK (GAP-03): `now` is the Redis SERVER time (redis.call('TIME') → {seconds, microseconds}), NOT a
-- caller-supplied timestamp. The whole design's premise is "one shared key = true global", but a
-- per-pod System.currentTimeMillis() let each gateway pod's wall-clock drift corrupt the shared window:
-- a fast-clocked pod inserts future-dated ZSET members that other pods' `now - window` prune never
-- removes until real time catches up. Sourcing `now` inside the script gives the window ONE authoritative
-- clock, regardless of how many pods share the key. Safe under Redis effect-replication (>= 5.0; lab is
-- 7.4): the script's effects, not the non-deterministic TIME call, are what replicate.
local key    = KEYS[1]
local seqKey = KEYS[2]
local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])

local t   = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

-- Drop entries older than the window.
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

local count = redis.call('ZCARD', key)
if count < limit then
  -- Unique member per request so concurrent calls in the same millisecond all count.
  redis.call('ZADD', key, now, now .. '-' .. redis.call('INCR', seqKey))
  redis.call('PEXPIRE', key, window)
  redis.call('PEXPIRE', seqKey, window)
  return limit - count - 1
end

return -1
