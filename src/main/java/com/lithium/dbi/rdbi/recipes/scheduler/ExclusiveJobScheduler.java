package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.lithium.dbi.rdbi.*;
import org.joda.time.Instant;

import java.util.List;

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

    public boolean schedule(final String tube, final String jobStr, final int ttlInMillis) {

        Handle handle = rdbi.open();
        try {
            return 1 == handle.attach(ExclusiveJobSchedulerDAO.class).scheduleJob(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    jobStr,
                    Instant.now().getMillis() + ttlInMillis);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> reserveMulti(final String tube, final long ttrInMillis, final int maxNumberOfJobs) {
        Handle handle = rdbi.open();
        try {
            return handle.attach(ExclusiveJobSchedulerDAO.class).reserveJobs(
                    getReadyQueue(tube),
                    getRunningQueue(tube),
                    maxNumberOfJobs,
                    Instant.now().getMillis(),
                    Instant.now().getMillis() + ttrInMillis);
        } finally {
            handle.close();
        }
    }

    public JobInfo reserveSingle(final String tube, final long ttrInMillis) {

        List<JobInfo> jobs = reserveMulti(tube, ttrInMillis, 1);

        //todo how shall we handle more than one job error mode?

        if (jobs == null || jobs.isEmpty()) {
            return null;
        } else {
            return jobs.get(0);
        }
    }

    public boolean deleteJob(final String tube, String jobStr) {
        Handle handle = rdbi.open();
        try {
            return 1 == handle.attach(ExclusiveJobSchedulerDAO.class)
                              .deleteJob(getReadyQueue(tube), getRunningQueue(tube), jobStr);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> removeExpiredJobs(String tube) {
        Handle handle = rdbi.open();
        try {
            return handle.attach(ExclusiveJobSchedulerDAO.class)
                         .removeExpiredJobs(getRunningQueue(tube), Instant.now().getMillis());
        } finally {
            handle.close();
        }
    }

    private String getRunningQueue(String tube) {
        return prefix + tube + ":running_queue";
    }

    private String getReadyQueue(String tube) {
        return prefix + tube + ":ready_queue";
    }

    @VisibleForTesting
    void nukeForTest(final String tube) {
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(getReadyQueue(tube), getRunningQueue(tube));
                return null;
            }
        });
    }
}
