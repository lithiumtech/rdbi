package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.ImmutableList;
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
                    getRunningQueue(tube),
                    getReadyAndRunningQueue(tube),
                    getPaused(tube),
                    jobStr,
                    Instant.now().getMillis() + runInMillis);
        }
    }

    @Override
    public List<TimeJobInfo> reserveMulti(final String tube, final long considerExpiredAfterMillis, final int maxNumberOfJobs) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(StateDedupedJobSchedulerDAO.class).reserveJobs(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    getPaused(tube),
                    maxNumberOfJobs,
                    Instant.now().getMillis(),
                    Instant.now().getMillis() + considerExpiredAfterMillis);
        }
    }

    @Override
    public boolean deleteJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .deleteJob(getReadyQueue(tube),
                                         getRunningQueue(tube),
                                         getReadyAndRunningQueue(tube),
                                         jobStr);
        }
    }

    public boolean deleteJobFromReady(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .deleteJobFromReady(getReadyQueue(tube),
                                                  getReadyAndRunningQueue(tube),
                                                  jobStr);
        }
    }

    public boolean ackJob(final String tube, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .ackJob(getReadyQueue(tube),
                                      getRunningQueue(tube),
                                      getReadyAndRunningQueue(tube),
                                      jobStr);
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
     * @param tube the name of a tube for related jobs
     * @param expirationPeriodInMillis the age in milliseconds beyond which a job is considered expired.
     * @return expired jobs that were removed.
     */
    public List<TimeJobInfo> removeExpiredReadyJobs(String tube, long expirationPeriodInMillis) {
        try (Handle handle = rdbi.open()) {
            final StateDedupedJobSchedulerDAO dao = handle.attach(StateDedupedJobSchedulerDAO.class);
            final ImmutableList.Builder<TimeJobInfo> builder = ImmutableList.builder();

            builder.addAll(dao.removeExpiredJobs(
                    getReadyQueue(tube),
                    Instant.now().minus(expirationPeriodInMillis).getMillis()));

            return builder.addAll(dao.removeExpiredJobs(
                    getReadyAndRunningQueue(tube),
                    Instant.now().minus(expirationPeriodInMillis).getMillis())).build();

        }
    }

    public boolean inReadyQueue(String tube, String jobStr) {
        return inQueue(getReadyQueue(tube), jobStr);
    }

    public boolean inRunningQueue(String tube, String jobStr) {
       return inQueue(getRunningQueue(tube), jobStr);
    }

    public boolean inReadyAndRunningQueue(String tube, String jobStr) {
        return inQueue(getReadyAndRunningQueue(tube), jobStr);
    }

    private boolean inQueue(String queueName, String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                              .inQueue(queueName, jobStr);
        }
    }

    protected String getReadyAndRunningQueue(String tube) {
        return getPrefix() + tube + ":ready_and_running_queue";
    }

}
