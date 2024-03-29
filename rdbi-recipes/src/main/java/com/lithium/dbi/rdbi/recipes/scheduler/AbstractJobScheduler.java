package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import redis.clients.jedis.resps.Tuple;

import java.time.Instant;
import java.util.List;

public abstract class AbstractJobScheduler<T extends JobInfo> {

    protected final RDBI rdbi;
    protected final String prefix;

    /**
     * @param rdbi the rdbi driver
     * @param redisPrefixKey the prefix key for the job system. All keys the job system uses will have the prefix redisPrefixKey
     */
    public AbstractJobScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    /**
     * Get the redis prefix key for this scheduler.  This is primarily to help facilitate testing if the need ever arises to
     * verify that the correct queue was chosen based on some business logic.
     *
     * @return the prefix key for the job system.
     */
    public String getPrefix() {
        return prefix;
    }

    public abstract List<T> reserveMulti(final String tube, final long timeToReserveMillis, final int maxNumberOfJobs);

    /**
     * Reserve a single job.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @param timeToReserveMillis The duration of the reservation in milliseconds, after which time the job becomes available to be put back into the ready queue again if not deleted first.
     * @return The job that has been reserved
     */
    public T reserveSingle(final String tube, final long timeToReserveMillis) {
        List<T> jobs = reserveMulti(tube, timeToReserveMillis, 1);
        if (jobs == null || jobs.isEmpty()) {
            return null;
        } else {
            return jobs.get(0);
        }
    }

    /**
     * Delete the job from the running queue.  If you are familiar with ack/nack style messaging systems, this is just like
     * ack-ing for your job.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @param jobStr A string representation of the job to be deleted
     * @return true if the job was successfully deleted from the running queue, false otherwise.
     */
    public boolean deleteRunningJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle
                    .attach(JobSchedulerDAO.class)
                    .deleteRunningJob(getRunningQueue(tube), jobStr);
        }
    }

    /**
     * Get the current number of jobs that have been reserved.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @return The number of reserved jobs
     */
    public long runningSize(String tube) {
        return getSortedSetSize(getRunningQueue(tube));
    }

    /**
     * Get the current number of jobs available to be reserved.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @return The number of available (ready) jobs to be reserved
     */
    public long readySize(String tube) {
        return getSortedSetSize(getReadyQueue(tube));
    }


    public List<T> peekDelayed(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), (double) Instant.now().toEpochMilli(), Double.MAX_VALUE, offset, count);
    }

    public List<T> peekReady(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), 0.0d, (double) Instant.now().toEpochMilli(), offset, count);
    }

    public List<T> peekRunning(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), (double) Instant.now().toEpochMilli(), Double.MAX_VALUE, offset, count);
    }

    public List<T> peekExpired(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), 0.0d, (double) Instant.now().toEpochMilli(), offset, count);
    }

    protected String getRunningQueue(String tube) {
        return prefix + tube + ":running_queue";
    }

    protected String getReadyQueue(String tube) {
        return prefix + tube + ":ready_queue";
    }

    protected List<T> peekInternal(String queue, Double min, Double max, int offset, int count) {
        List<T> jobInfos = Lists.newArrayList();
        try (Handle handle = rdbi.open()) {
            List<Tuple> tupleList = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            for (Tuple tuple : tupleList) {
                jobInfos.add(createJobInfo(tuple.getElement(), tuple.getScore()));
            }
            return jobInfos;
        }
    }

    protected abstract T createJobInfo(String jobStr, double jobScore);

    private long getSortedSetSize(final String key) {
        return rdbi.withHandle(handle -> handle.jedis().zcard(key));
    }

}
