package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

//CR: Need a class-level javadoc explaining what this does and pointing reader to appropriate readme docs for more detail.

/**
 * Implements a scheduled job system that de-duplicates jobs. A job system consists of one or more tubes. Each tube two
 * queue, a ready queue and a running queue. To shedule a job on the ready queue, use schedule( jobStr, ttlInMillis). When the
 * job is ready, you can reserve the job to run via reserve(ttrInMillis). When a job is reserved it has a count down of
 * ttr, once ttr is up, the job is considered expired. Expired jobs can be extracted from the queue via cull(). To cancel
 * a job in the running or ready queue, use delete().
 *
 * TODO add tutorial, remember to add attribution to the idea
 * https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
 */
public class ExclusiveJobScheduler {

    //TODO think about runtime exception, the scheduler should catch all connection based one and handle them
    private final RDBI rdbi;
    private final String prefix;

    /**
     * @param rdbi the rdbi driver
     * @param redisPrefixKey the prefix key for the job system. All keys the job system uses will have the prefix redisPrefixKey
     */
    public ExclusiveJobScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    /**
     * This will "pause" the system for the specified tube, preventing any new jobs from being scheduled
     * or reserved.
     */
    public void pause(final String tube){
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().set(getPaused(tube), String.valueOf(System.currentTimeMillis()/1000));
                return null;
            }
        });
    }

    public boolean isPaused(final String tube){
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
     */
    public String getPauseStart(final String tube){
        return rdbi.withHandle(new Callback<String>() {
            @Override
            public String run(Handle handle) {
                return handle.jedis().get(getPaused(tube));
            }
        });
    }

    /**
     * This will resume / un-pause the system for the specified tube, allowing jobs to be scheduled and reserved.
     *
     */
    public void resume(final String tube){
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(getPaused(tube));
                return null;
            }
        });
    }

    public boolean schedule(final String tube, final String jobStr, final int ttlInMillis) {

        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(ExclusiveJobSchedulerDAO.class).scheduleJob(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    getPaused(tube),
                    jobStr,
                    Instant.now().getMillis() + ttlInMillis);
        }
    }

    public List<TimeJobInfo> reserveMulti(final String tube, final long ttrInMillis, final int maxNumberOfJobs) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(ExclusiveJobSchedulerDAO.class).reserveJobs(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    getPaused(tube),
                    maxNumberOfJobs,
                    Instant.now().getMillis(),
                    Instant.now().getMillis() + ttrInMillis);
        }
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

    public boolean deleteJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(ExclusiveJobSchedulerDAO.class)
                              .deleteJob(getReadyQueue(tube), getRunningQueue(tube), jobStr);
        }
    }

    public List<TimeJobInfo> removeExpiredJobs(String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(ExclusiveJobSchedulerDAO.class)
                         .removeExpiredJobs(getRunningQueue(tube), Instant.now().getMillis());
        }
    }

    /**
     * A job that has been sitting in the ready queue for the last "expirationPeriodInMillis"
     * is considered expired. Expired jobs are removed from the scheduler and returned to the client.
     */
    public List<TimeJobInfo> removeExpiredReadyJobs(String tube, long expirationPeriodInMillis) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(ExclusiveJobSchedulerDAO.class)
                         .removeExpiredJobs(getReadyQueue(tube), Instant.now().minus(expirationPeriodInMillis).getMillis());
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

        final List<TimeJobInfo> jobInfos = Lists.newArrayList();
        try (Handle handle = rdbi.open()) {
            Set<Tuple> tupleSet = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            for (Tuple tuple : tupleSet) {
                jobInfos.add(new TimeJobInfo(tuple.getElement(), tuple.getScore()));
            }
            return jobInfos;
        }
    }

    private long getJobCount(final String queue) {
        return rdbi.withHandle(new Callback<Long>() {
            @Override
            public Long run(Handle handle) {
                return handle.jedis().zcard(queue);
            }
        });
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
