package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;

import java.util.List;

/**
 * Similar to ExclusiveJobScheduler. Main difference is that jobs are de-duplicated by state.
 * So the same job might be in the ready queue and the running queue at the same time. However,
 * only one instance of a given job will ever be found in a particular state. This allows clients
 * to schedule a job to run immediately after the one currently in progress has been completed.
 *
 * When reserving multiple jobs, the scheduler will make sure it returns the requested number of jobs as long
 * as they are available to be reserved in the ready queue. This is an important difference from how the
 * PriorityBasedJobScheduler and TimeBasedScheduler work. Those schedulers will attempt to reserve
 * the number of jobs requested only once. If any of the jobs found in the ready queue are in the running queue those
 * will not be reserved. No further attempts will be made to reserve the number of jobs requested.
 */
public class StateDedupedJobScheduler extends AbstractDedupJobScheduler {

    /**
     * @param rdbi the rdbi driver
     * @param redisPrefixKey the prefix key for the job system. All keys the job system uses will have the prefix redisPrefixKey
     */
    public StateDedupedJobScheduler(RDBI rdbi, String redisPrefixKey) {
        super(rdbi, redisPrefixKey);
    }

    /**
     * @return true if the job was scheduled.
     *         false indicates the job already exists in the ready queue or the running queue.
     */
    @Override
    public boolean schedule(final String tube, final String jobStr, final int runInMillis) {

        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class).scheduleJob(
                    getReadyQueue(tube),
                    getPaused(tube),
                    jobStr,
                    Instant.now().getMillis() + runInMillis);
        }
    }

    @Override
    public List<TimeJobInfo> reserveMulti(final String tube, final long ttrInMillis, final int maxNumberOfJobs) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(StateDedupedJobSchedulerDAO.class).reserveJobs(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    getPaused(tube),
                    maxNumberOfJobs,
                    Instant.now().getMillis(),
                    Instant.now().getMillis() + ttrInMillis);
        }
    }

    @Override
    public boolean deleteJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .deleteJob(getReadyQueue(tube), getRunningQueue(tube), jobStr);
        }
    }

    public boolean ackJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .ackJob(getRunningQueue(tube), jobStr);
        }
    }

    public List<TimeJobInfo> removeExpiredRunningJobs(String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(StateDedupedJobSchedulerDAO.class)
                         .removeExpiredJobs(getRunningQueue(tube), Instant.now().getMillis());
        }
    }

    /**
     * A job that has been sitting in the ready queue for the last "expirationPeriodInMillis"
     * is considered expired. Expired jobs are removed from the scheduler and returned to the client.
     */
    public List<TimeJobInfo> removeExpiredReadyJobs(String tube, long expirationPeriodInMillis) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(StateDedupedJobSchedulerDAO.class)
                         .removeExpiredJobs(getReadyQueue(tube), Instant.now().minus(expirationPeriodInMillis).getMillis());
        }
    }

    public boolean inReadyQueue(String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                         .inQueue(getReadyQueue(tube), jobStr);
        }
    }

    public boolean inRunningQueue(String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .inQueue(getRunningQueue(tube), jobStr);
        }
    }
}
