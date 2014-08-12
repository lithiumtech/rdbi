package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface JobSchedulerDAO {

    /**
     * Adds an item to the sorted set of jobs not yet processed. The set is sorted by the provided timestamp.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param job Key for sorted set.
     * @param timestamp Earliest UTC timestamp to process this job.
     * @return
     */
    @Query("return redis.call('ZADD', $readyQueue$, $timestamp$, $jobStr$)")
    public int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindArg("jobStr") String job,
            @BindArg("timestamp") long timestamp);

    /**
     * Moves items from readyQueue to runningQueue and returns details. The caller is taking
     * responsibility for processing the returned job details and should call
     * {@link #deleteRunningJob(String, String)} for each job it completes.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param runningQueue Sorted set name for items currently being processed.
     * @param limit Maximum number of jobs to accept.
     * @param timestamp Maximum timestamp for jobs to retrieve items from readyQueue.
     * @param expirationTime Timestamp in the future at which the retrieved jobs should be considered expired.
     * @return Jobs that must be processed by the caller.
     */
    @Mapper(JobInfoListMapper.class)
    @Query(
        "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
        "if next(jobs) == nil then\n" +
        "    return nil\n" +
        "end\n" +
        "for i=1,2*#jobs,2 do\n" +
        "    redis.call('ZREM', $readyQueue$, jobs[i])\n" + //Note: in order to support "limit", we have to loop the delete, perhaps not have limit, 1 at a time?
        "    redis.call('ZADD', $runningQueue$, $expiration$, jobs[i])\n" +
        "end\n" +
        "return jobs"
    )
    public List<JobInfo> reserveJobs(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("limit") int limit,
            @BindArg("now") long timestamp,
            @BindArg("expiration") long expirationTime);

    /**
     * Move all expired jobs from the runningQueue to the readyQueue.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param runningQueue Sorted set name for items currently being processed.
     * @param timestamp Expiration time. Generally this should be the current time in milliseconds.
     * @return Jobs moved back to readyQueue.
     */
    @Mapper(JobInfoListMapper.class)
    @Query(
        "local expiredJobs = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $timestamp$, 'WITHSCORES')\n" +
        "for i=1,2*#expiredJobs,2 do\n" +
        "    redis.call('ZADD', $readyQueue$, $timestamp$, expiredJobs[i])\n" +
        "end\n" +
        "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $timestamp$)\n" +
        "return expiredJobs"
    )
    public List<JobInfo> requeueExpiredJobs(@BindKey("readyQueue") String readyQueue, @BindKey("runningQueue") String runningQueue, @BindArg("timestamp") Long timestamp);

    /**
     * Delete a job from the runningQueue. This implies the job is no longer being executed.
     *
     * @param runningQueue Sorted set name for items currently being processed.
     * @param job
     * @return 1 if successful, or 0 if unsuccessful.
     */
    @Query("return redis.call('ZREM', $runningQueue$, $job$)")
    public int deleteRunningJob(
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);
}
