package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AbstractDedupJobScheduler {
    protected final RDBI rdbi;
    private final String prefix;

    public AbstractDedupJobScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    public abstract boolean schedule(final String tube, final String jobStr, final int ttlInMillis);
    public abstract List<TimeJobInfo> reserveMulti(final String tube, final long ttrInMillis, final int maxNumberOfJobs);
    public abstract boolean deleteJob(final String tube, String jobStr);

    /**
     * This will "pause" the system for the specified tube, preventing any new jobs from being scheduled
     * or reserved.
     */
    public void pause(final String tube) {
        rdbi.consumeHandle(handle -> handle.jedis().set(getPaused(tube), String.valueOf(System.currentTimeMillis() / 1000)));
    }

    public boolean isPaused(final String tube) {
        return rdbi.withHandle(handle -> handle.jedis().get(getPaused(tube)) != null);
    }

    /**
     * This returns the value for the pause key. If that value was created through this library
     * it will be a unix timestamp (seconds since the epoch).
     */
    public String getPauseStart(final String tube) {
        return rdbi.withHandle(handle -> handle.jedis().get(getPaused(tube)));
    }

    /**
     * This will resume / un-pause the system for the specified tube, allowing jobs to be scheduled and reserved.
     */
    public void resume(final String tube) {
        rdbi.consumeHandle(handle -> handle.jedis().del(getPaused(tube)));
    }

    public TimeJobInfo reserveSingle(final String tube, final long ttrInMillis) {

        List<TimeJobInfo> jobs = reserveMulti(tube, ttrInMillis, 1);

        //todo how shall we handle more than one job error mode?

        if (jobs == null || jobs.isEmpty()) {
            return null;
        } else {
            return jobs.get(0);
        }
    }

    public long getReadyJobCount(String tube) {
        return getJobCount(getReadyQueue(tube));
    }

    public long getRunningJobCount(String tube) {
        return getJobCount(getRunningQueue(tube));
    }

    public List<TimeJobInfo> peekDelayed(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), new Double(Instant.now().getMillis()), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekReady(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), 0.0d, new Double(Instant.now().getMillis()), offset, count);
    }

    public List<TimeJobInfo> peekRunning(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), new Double(Instant.now().getMillis()), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekExpired(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), 0.0d, new Double(Instant.now().getMillis()), offset, count);
    }

    private List<TimeJobInfo> peekInternal(String queue, Double min, Double max, int offset, int count) {
        final List<TimeJobInfo> jobInfos = new ArrayList<>();
        try (Handle handle = rdbi.open()) {
            Set<Tuple> tupleSet = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            for (Tuple tuple : tupleSet) {
                jobInfos.add(new TimeJobInfo(tuple.getElement(), tuple.getScore()));
            }
            return jobInfos;
        }
    }

    private long getJobCount(final String queue) {
        return rdbi.withHandle(handle -> handle.jedis().zcard(queue));
    }

    protected String getRunningQueue(String tube) {
        return prefix + tube + ":running_queue";
    }

    protected String getReadyQueue(String tube) {
        return prefix + tube + ":ready_queue";
    }

    protected String getPaused(String tube){
        return prefix + tube + ":paused";
    }
}