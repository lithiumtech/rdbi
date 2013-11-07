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
                handle.jedis().set(getPaused(tube), "true");
                return null;
            }
        });
    }

    public boolean isPaused(final String tube){
        return isPaused(tube, null);
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

        Handle handle = rdbi.open();
        try {
            if(isPaused(tube, handle)){
                return false;
            } else {
                return 1 == handle.attach(ExclusiveJobSchedulerDAO.class).scheduleJob(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        jobStr,
                        Instant.now().getMillis() + ttlInMillis);
            }
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> reserveMulti(final String tube, final long ttrInMillis, final int maxNumberOfJobs) {
        Handle handle = rdbi.open();
        try {
            if(isPaused(tube, handle)){
                return null;
            } else {
                return handle.attach(ExclusiveJobSchedulerDAO.class).reserveJobs(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        maxNumberOfJobs,
                        Instant.now().getMillis(),
                        Instant.now().getMillis() + ttrInMillis);
            }
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

    protected String getPaused(String tube){
        return prefix + tube + ":paused";
    }

    private boolean isPaused(final String tube, Handle handle){
        if(handle == null) {
            return rdbi.withHandle(new Callback<Boolean>() {
                @Override
                public Boolean run(Handle handle) {
                    String stopped = handle.jedis().get(getPaused(tube));
                    return Boolean.valueOf(stopped);
                }
            });
        } else {
            String stopped = handle.jedis().get(getPaused(tube));
            return Boolean.valueOf(stopped);
        }
    }
}
