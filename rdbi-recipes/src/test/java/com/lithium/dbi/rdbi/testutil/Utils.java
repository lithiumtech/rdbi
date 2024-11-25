package com.lithium.dbi.rdbi.testutil;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.testng.AssertJUnit.fail;

public class Utils {

    public static String uniqueTubeName() {
        return "test_tube_" + UUID.randomUUID().toString();
    }

    public static void assertTiming(long timeLimit, TimeUnit unit, Runnable... runnables) {
        long timeOut = timeLimit * 10;
        long start = System.currentTimeMillis();


        List<CompletableFuture<Void>> futures = Arrays.stream(runnables)
                                                      .map(CompletableFuture::runAsync)
                                                      .collect(Collectors.toList());

        CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));

        try {
            all.get(timeOut, unit);
        } catch (TimeoutException e) {
            fail(String.format("did not complete within time limit %d %s, or timeout %d %s", timeLimit, unit, timeOut, unit));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        long elapsed = System.currentTimeMillis() - start;
        if (elapsed > timeLimit) {
            fail(String.format("Did not finish in time. Expected to finish in %d %s, but finished in %d", timeLimit, unit, elapsed));
        }
    }

}
