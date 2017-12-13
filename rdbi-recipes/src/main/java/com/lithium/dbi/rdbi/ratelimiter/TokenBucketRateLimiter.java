package com.lithium.dbi.rdbi.ratelimiter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Rate limiter implementation based on
 * https://en.wikipedia.org/wiki/Token_bucket that generally adheres
 * to an average rate limit but allows for a configurable burst bucket to
 * better accommodate uneven workloads
 */
public class TokenBucketRateLimiter implements RateLimiter {

    private static final Logger logger = LoggerFactory.getLogger(TokenBucketRateLimiter.class);
    private static final String LUA_SCRIPT = loadScript();
    private static final String LUA_SCRIPT_SHA1 = DigestUtils.sha1Hex(LUA_SCRIPT);

    private final String fullyQualifiedKey;
    private final RDBI rdbi;
    private final int maxTokens;

    private final double refillRatePerMs;
    private final LongSupplier clock;

    public TokenBucketRateLimiter(RDBI rdbi,
                                  String keyPrefix,
                                  String key,
                                  int maxTokens,
                                  int refillValue,
                                  Duration refillPeriod,
                                  LongSupplier clock
                                 ) {
        checkArgument(maxTokens > 0, "Max tokens %s must be > 0", maxTokens);
        checkArgument(refillValue > 0, "Refill value of %s must be > 0", refillValue);
        checkArgument(refillPeriod.toMillis() > 0, "Refill period of %ss must be > 0s", refillPeriod.toMillis() / 1000);

        this.rdbi = rdbi;
        this.maxTokens = maxTokens;
        this.refillRatePerMs = refillValue * 1.0 / refillPeriod.toMillis();
        fullyQualifiedKey = Joiner.on(":").join(keyPrefix, "tokenBucketRateLimit", key);
        this.clock = clock;
    }

    public TokenBucketRateLimiter(RDBI rdbi,
                                  String keyPrefix,
                                  String key,
                                  int maxTokens,
                                  int refillValue,
                                  Duration refillPeriod
                                 ) {
        this(rdbi, keyPrefix, key, maxTokens, refillValue, refillPeriod, System::currentTimeMillis);
    }


    private static String loadScript() {
        try {
            return Resources.toString(Resources.getResource("token-bucket.lua"), Charsets.UTF_8).trim();
        } catch (IOException e) {
            logger.error("Could not load token-bucket.lua", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public OptionalLong getWaitTimeForPermit() {
      return getWaitTimeForPermits(1);
    }

    @VisibleForTesting
    OptionalLong getWaitTimeForPermits(int requestedPermits) {

        try (Handle handle = rdbi.open()) {
            final Jedis jedis = handle.jedis();

            while (true) {
                long evalResult;
                try {
                    // Use the sha to run the LUA script for maximum performance.  Have a fall-back to reload the script
                    // in the event of a redis master/slave failover or upon initial execution.
                    Object tmp = jedis.evalsha(LUA_SCRIPT_SHA1,
                                               1,
                                               fullyQualifiedKey,
                                               String.valueOf(requestedPermits),
                                               String.valueOf(maxTokens),
                                               String.valueOf(refillRatePerMs),
                                               String.valueOf(clock.getAsLong())
                                              );
                    evalResult = (long) tmp;
                } catch (JedisDataException jde) {
                    if (jde.getMessage() != null && jde.getMessage().contains("NOSCRIPT")) {
                        jedis.scriptLoad(LUA_SCRIPT);
                        continue;
                    } else {
                        throw jde;
                    }
                }

                if (evalResult > 0) {
                    // We are good!
                    return OptionalLong.empty();
                }

                // We are over our allotment. The return value is the negative of the number of milliseconds we should wait.
                return OptionalLong.of(-1 * evalResult);
            }
        }
    }

    @Override
    public String getKey() {
        return fullyQualifiedKey;
    }
}
