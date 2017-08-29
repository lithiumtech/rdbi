package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public abstract class AbstractDedupJobScheduler {
    protected final RDBI rdbi;
    private final String prefix;

    public AbstractDedupJobScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    /**
     * schedule a job (place in the ready queue, and schedule for some number of millis after "now").
     * @param tube job group. jobs within the same job group are deduped by job string.
     * @param jobStr string representing the job. jobs with identical strings are deduplicated.
     * @param becomeReadyInMillis this job will become ready to run this many milliseconds into the future.
     * @return true if the job was scheduled.
     *         false indicates the job already exists in the ready queue or the running queue.
     */
    public abstract boolean schedule(final String tube, final String jobStr, final int becomeReadyInMillis);

    /**
     * reserve a number of "ready" jobs (in the "ready" queue, and scheduled for some time before "now"),
     * moving them to the "running" queue.
     * @param tube job group. we will only grab ready jobs from this group.
     * @param considerExpiredAfterMillis if jobs haven't been deleted after being reserved for this many millis, consider them expired.
     * @param maxNumberOfJobs number of jobs to reserve.
     * @return list of jobs reserved (now considered "running",) or empty list if none.
     */
    public abstract List<TimeJobInfo> reserveMulti(final String tube, final long considerExpiredAfterMillis, final int maxNumberOfJobs);

    /**
     * delete job from ready AND running queues, regardless of job state.
     * @param tube job group. jobs within the same job group are deduped by job string.
     * @param jobStr string representing the job. jobs with identical strings are deduplicated.
     * @return true if deleted ok, false if job wasn't found in either ready or running.
     */
    public abstract boolean deleteJob(final String tube, String jobStr);

    /**
     * This will "pause" the system for the specified tube, preventing any new jobs from being scheduled
     * or reserved.
     * @param tube the name of related jobs
     */
    public void pause(final String tube) {
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().set(getPaused(tube), String.valueOf(System.currentTimeMillis() / 1000));
                return null;
            }
        });
    }

    public boolean isPaused(final String tube) {
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                return handle.jedis().get(getPaused(tube)) != null;
            }
        });
    }

    /**
     * This returns the value for the pause key. If that value was created through this library
     * it will be a unix timestamp (seconds since the epoch).
     * @param tube the name of related jobs
     * @return the time in epoch seconds the tube was paused.
     */
    public String getPauseStart(final String tube) {
        return rdbi.withHandle(new Callback<String>() {
            @Override
            public String run(Handle handle) {
                return handle.jedis().get(getPaused(tube));
            }
        });
    }

    /**
     * This will resume / un-pause the system for the specified tube, allowing jobs to be scheduled and reserved.
     * @param tube the name of related jobs
     */
    public void resume(final String tube) {
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(getPaused(tube));
                return null;
            }
        });
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
        final String queue = getReadyQueue(tube);
        final long now = Instant.now().getMillis();
        return rdbi.withHandle(new Callback<Long>() {
            @Override
            public Long run(Handle handle) {
                return handle.jedis().zcount(queue, 0, now);
            }
        });
    }

    public long getRunningJobCount(String tube) {
        final String queue = getRunningQueue(tube);
        return rdbi.withHandle(new Callback<Long>() {
            @Override
            public Long run(Handle handle) {
                return handle.jedis().zcard(queue);
            }
        });
    }

    public List<TimeJobInfo> peekDelayed(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), (double) Instant.now().getMillis(), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekReady(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), 0.0d, (double) Instant.now().getMillis(), offset, count);
    }

    public List<TimeJobInfo> peekRunning(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), (double) Instant.now().getMillis(), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekExpired(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), 0.0d, (double) Instant.now().getMillis(), offset, count);
    }

    private List<TimeJobInfo> peekInternal(String queue, Double min, Double max, int offset, int count) {

        final List<TimeJobInfo> jobInfos = Lists.newArrayList();
        try (Handle handle = rdbi.open()) {
            Set<Tuple> tupleSet = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            for (Tuple tuple : tupleSet) {
                jobInfos.add(new TimeJobInfo(tuple.getElement(), tuple.getScore()));
            }
            return jobInfos;
        }
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