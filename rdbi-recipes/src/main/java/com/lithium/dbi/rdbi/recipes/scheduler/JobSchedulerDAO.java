package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface JobSchedulerDAO {

    /**
     * Adds an item to the readyQueue, which is a sorted set of jobs not yet
     * processed. This set issorted by the provided timestamp. If item already
     * exists in the readyQueue, the timestamp will be updated if it is &gt; the
     * original timestamp and also within quiescence milliseconds of the
     * original timestamp.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param job Key for sorted set.
     * @param timestamp Earliest UTC timestamp to process this job.
     * @param quiescence Maximum forward distance (exclusive) from current timestamp to allow an update if the job already exists.
     * @return 1 if added or updated, 0 otherwise.
     */
    @Query(
            "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
            "if not readyJobScore or ($timestamp$ > readyJobScore and math.abs($timestamp$ - readyJobScore) < tonumber($quiescence$)) then\n" +
            "    redis.call('ZADD', $readyQueue$, $timestamp$, $jobStr$)\n" +
            "    return 1\n" +
            "else\n" +
            "    return 0\n" +
            "end"
    )
    public int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindArg("jobStr") String job,
            @BindArg("timestamp") long timestamp,
            @BindArg("quiescence") long quiescence);

    /**
     * Moves items from readyQueue to runningQueue and returns details.
     * The caller is taking responsibility for processing the returned job
     * details and should call {@link #deleteRunningJob(String, String)} for
     * each job it completes. If a duplicate job is already in runningQueue,
     * it will not be removed from readyQueue.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param runningQueue Sorted set name for items currently being processed.
     * @param limit Maximum number of jobs to accept.
     * @param lowerScore Minimum score for jobs to retrieve from readyQueue
     * @param upperScore Maximum score for jobs to retrieve from readyQueue.
     * @param expirationTime Timestamp in the future at which the retrieved jobs should be considered expired.
     * @return Jobs that must be processed by the caller.
     */
    @Mapper(JobInfoListMapper.class)
    @Query(
        "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, $lowerScore$, $upperScore$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
        "if next(jobs) == nil then\n" +
        "    return nil\n" +
        "end\n" +
        "local reserved = {}\n" +
        "local reservedIndex = 1\n" +
        "for i=1,#jobs,2 do\n" +
        "    local runningJob = redis.call('ZSCORE', $runningQueue$, jobs[i])\n" +
        "    if not runningJob then\n" +
        "        reserved[reservedIndex] = jobs[i]\n" +
        "        reserved[reservedIndex + 1] = jobs[i + 1]\n" +
        "        reservedIndex = reservedIndex + 2\n" +
        "        redis.call('ZREM', $readyQueue$, jobs[i])\n" +
        "        redis.call('ZADD', $runningQueue$, $expiration$, jobs[i])\n" +
        "    end\n" +
        "end\n" +
        "return reserved"
    )
    public List<JobInfo> reserveJobs(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("limit") int limit,
            @BindArg("lowerScore") long lowerScore,
            @BindArg("upperScore") long upperScore,
            @BindArg("expiration") long expirationTime);

    /**
     * Move all expired jobs from the runningQueue to the readyQueue.
     *
     * @param readyQueue Sorted set name for items not yet processed.
     * @param runningQueue Sorted set name for items currently being processed.
     * @param timestamp Expiration time. Generally this should be the current time in milliseconds.
     * @param newScore the new score to apply to moved jobs.
     * @return Jobs moved back to readyQueue.
     */
    @Mapper(JobInfoListMapper.class)
    @Query(
        "local expiredJobs = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $timestamp$, 'WITHSCORES')\n" +
        "for i=1,#expiredJobs,2 do\n" +
        "    redis.call('ZADD', $readyQueue$, $newScore$, expiredJobs[i])\n" +
        "end\n" +
        "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $timestamp$)\n" +
        "return expiredJobs"
    )
    public List<JobInfo> requeueExpiredJobs(@BindKey("readyQueue") String readyQueue,
                                            @BindKey("runningQueue") String runningQueue,
                                            @BindArg("timestamp") long timestamp,
                                            @BindArg("newScore") double newScore);

    /**
     * Delete a job from the runningQueue. This implies the job is no longer
     * being executed.
     *
     * @param runningQueue Sorted set name for items currently being processed.
     * @param job the string value of a running job.
     * @return 1 if successful, or 0 if unsuccessful.
     */
    @Query("return redis.call('ZREM', $runningQueue$, $job$)")
    public int deleteRunningJob(
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);
}
