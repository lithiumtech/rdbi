package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

/**
 * Differences between this class and {@link com.lithium.dbi.rdbi.recipes.scheduler.ExclusiveJobScheduler}:
 * <ul>
 *     <li>Repeatedly calling schedule will succeed each time, merely updating the score in Redis.</li>
 *     <li>The delete operation does not delete from the readyQueue, but only the runningQueue.</li>
 *     <li>Expired jobs in the runningQueue are not purged, but instead moved back to the readyQueue.</li>
 * </ul>
 */
public class JobScheduler {

    private final RDBI rdbi;
    private final String prefix;

    /**
     * @param rdbi the rdbi driver
     * @param redisPrefixKey the prefix key for the job system. All keys the job system uses will have the prefix redisPrefixKey
     */
    public JobScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    public boolean schedule(final String tube, final String jobStr, final int millisInFuture, final int quiescence) {
        Handle handle = rdbi.open();
        try {
            return 1 == handle
                    .attach(JobSchedulerDAO.class)
                    .scheduleJob(
                            getReadyQueue(tube),
                            jobStr,
                            Instant.now().getMillis() + millisInFuture,
                            quiescence);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> reserveMulti(final String tube, final long timeToReserveMillis, final int maxNumberOfJobs) {
        Handle handle = rdbi.open();
        try {
            return handle
                    .attach(JobSchedulerDAO.class)
                    .reserveJobs(
                            getReadyQueue(tube),
                            getRunningQueue(tube),
                            maxNumberOfJobs,
                            Instant.now().getMillis(),
                            Instant.now().getMillis() + timeToReserveMillis);
        } finally {
            handle.close();
        }
    }

    public JobInfo reserveSingle(final String tube, final long ttrInMillis) {

        List<JobInfo> jobs = reserveMulti(tube, ttrInMillis, 1);
        if (jobs == null || jobs.isEmpty()) {
            return null;
        } else {
            return jobs.get(0);
        }
    }

    public boolean deleteRunningJob(final String tube, String jobStr) {
        Handle handle = rdbi.open();
        try {
            return 1 == handle
                    .attach(JobSchedulerDAO.class)
                    .deleteRunningJob(getRunningQueue(tube), jobStr);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> requeueExpired(final String tube) {
        Handle handle = rdbi.open();
        try {
            return handle
                    .attach(JobSchedulerDAO.class)
                    .requeueExpiredJobs(getReadyQueue(tube), getRunningQueue(tube), Instant.now().getMillis());
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> peekDelayed(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), new Double(Instant.now().getMillis()), Double.MAX_VALUE, offset, count);
    }

    public List<JobInfo> peekReady(String tube, int offset, int count) {
        return peekInternal(getReadyQueue(tube), 0.0d, new Double(Instant.now().getMillis()), offset, count);
    }

    public List<JobInfo> peekRunning(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), new Double(Instant.now().getMillis()), Double.MAX_VALUE, offset, count);
    }

    public List<JobInfo> peekExpired(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), 0.0d, new Double(Instant.now().getMillis()), offset, count);
    }

    private List<JobInfo> peekInternal(String queue, Double min, Double max, int offset, int count) {

        List<JobInfo> jobInfos = Lists.newArrayList();
        Handle handle = rdbi.open();
        try {
            Set<Tuple> tupleSet = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            for (Tuple tuple : tupleSet) {
                jobInfos.add(new JobInfo(tuple.getElement(), new Instant((long) tuple.getScore())));
            }
            return jobInfos;
        } finally {
            handle.close();
        }
    }

    protected String getRunningQueue(String tube) {
        return prefix + tube + ":running_queue";
    }

    protected String getReadyQueue(String tube) {
        return prefix + tube + ":ready_queue";
    }

}
