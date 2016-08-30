package com.lithium.dbi.rdbi.recipes.cache;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * Primary implementation of {@link SimpleMultiCache} that uses Redis as its caching layer.
 * There is no attempt to prevent multiple load functions from running simultaneously.
 */
public class SimpleMultiCacheRedis implements SimpleMultiCache {

    private final Logger logger = LoggerFactory.getLogger(SimpleMultiCacheRedis.class);

    private final ObjectMapper jsonMapper;
    private final RDBI rdbi;

    public SimpleMultiCacheRedis(ObjectMapper jsonMapper, RDBI rdbi) {
        this.jsonMapper = jsonMapper;
        this.rdbi = rdbi;
    }

    @Override
    public <T> T loadWithFallback(String cacheKey,
                                  TypeReference<T> clazz,
                                  Callable<T> loadFunction,
                                  int cacheTimeToLive,
                                  CacheMetrics cacheMetrics) throws Exception {
        T response = readFromCache(cacheKey, clazz);
        final Optional<CacheMetrics> metricsOption = Optional.ofNullable(cacheMetrics);
        if (response != null) {
            // Successfully loaded from the cache!
            metricsOption.ifPresent(CacheMetrics::markHit);
            return response;
        }
        // Mark the cache miss
        metricsOption.ifPresent(CacheMetrics::markMiss);
        // Start a load timer
        final Optional<Timer.Context> queryTimer = metricsOption.map(CacheMetrics::time);
        // Load the data
        response = loadFunction.call();
        // Stop the load timer
        queryTimer.ifPresent(Timer.Context::stop);
        // Cache the result
        cacheObject(response, cacheKey, cacheTimeToLive);
        return response;
    }

    private void cacheObject(Object object, String cacheKey, int cacheTimeToLive) {
        try (Handle h = rdbi.open()) {
            h.jedis().set(cacheKey, jsonMapper.writeValueAsString(object), "NX", "EX", cacheTimeToLive);
        } catch (IOException ioe) {
            logger.error("Got IOException writing cached result for {}", cacheKey, ioe);
        }
    }

    @Nullable
    private <T> T readFromCache(String cacheKey, TypeReference<T> type) {
        try (Handle h = rdbi.open()) {
            String cacheValue = h.jedis().get(cacheKey);
            if (cacheValue != null) {
                return jsonMapper.readValue(cacheValue, type);
            }
        } catch (IOException ioe) {
            logger.error("Got IOException reading cached result for {}", cacheKey, ioe);
        }
        return null;
    }
}
