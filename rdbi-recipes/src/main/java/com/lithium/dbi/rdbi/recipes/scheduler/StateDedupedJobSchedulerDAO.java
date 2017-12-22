package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

/**
 * Main difference between this DAO and the ExclusiveJobSchedulerDAO is that this DAO
 * allows the same job to be in the running queue and ready queue at the same time.
 *
 * Also while reserving jobs, a job is only reserved if the job has no instances in the
 * running queue.
 */
public interface StateDedupedJobSchedulerDAO {
    /**
     * When scheduling a job make sure its not already in the ready queue. A job
     * can be scheduled regardless of whether its in the running queue.
     * @param readyQueue the name of the ready queue
     * @param runningQueue the name of the running queue
     * @param readyAndRunningQueue the name of the ready and running queue
     * @param pausedTube the name of the paused tube
     * @param job the string representation of a job to schedule
     * @param runInMillis the timestamp in milliseconds when the job should be run.
     * @return 1 if job was scheduled or 0 otherwise.
     */
    @Query(
        "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $job$)\n" +
        "local runningJobScore = redis.call('ZSCORE', $runningQueue$, $job$)\n" +
        "local readyAndRunningJobScore = redis.call('ZSCORE', $readyAndRunningQueue$, $job$)\n" +
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "if not readyJobScore and not readyAndRunningJobScore and not isPaused then\n" +
        "   if not runningJobScore then\n" +
        "     redis.call('ZADD', $readyQueue$, $runInMillis$, $job$)\n" +
        "   else\n" +
        "     redis.call('ZADD', $readyAndRunningQueue$, $runInMillis$, $job$)\n" +
        "   end\n" +
        "   return 1\n" +
        "else\n" +
        "   return 0\n" +
        "end"
    )
    int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("readyAndRunningQueue") String readyAndRunningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("job") String job,
            @BindArg("runInMillis") long runInMillis);

    /**
     * When reserving jobs reserve jobs that are currently in ready queue but not in the running queue.
     * If the job is in running queue need to wait till the running instance of the job has completed before
     * we allow another reservation of the job from the ready queue.
     * @param readyQueue the ready queue name
     * @param runningQueue the running queue name
     * @param pausedTube the paused queue tube name
     * @param limit max number of jobs to start
     * @param now current timestamp
     * @param ttl time to live for scheduled jobs
     * @return the list of jobs successfully scheduled.
     */
    @Mapper(TimeJobInfoListMapper.class)
    @Query(
        "local reserved = {}\n" +
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "if isPaused then\n" +
        "    return reserved\n" +
        "end\n" +
        "local reservedIndex = 1\n" +
        "local nextLimit = tonumber($limit$)\n" +
        "local nextOffset = 0\n" +
        "while nextLimit > 0 do\n" +
        "   local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', nextOffset, nextLimit)\n" +
        "   if next(jobs) == nil then\n" +
        "       return reserved\n" +
        "   end\n" +
        "   for i=1,#jobs,2 do\n" +
        "      local inRunningQueue = redis.call('ZSCORE', $runningQueue$, jobs[i])\n" +
        "      if not inRunningQueue then\n" +
        "          reserved[reservedIndex] = jobs[i]\n" +
        "          reserved[reservedIndex + 1] = jobs[i + 1]\n" +
        "          redis.call('ZREM', $readyQueue$, reserved[reservedIndex])\n" +
        "          redis.call('ZADD', $runningQueue$, $ttl$, reserved[reservedIndex])\n" +
        "          reservedIndex = reservedIndex + 2\n" +
        "          nextLimit = nextLimit - 1\n" +
        "      end\n" +
        "   end\n" +
        "   nextOffset = nextOffset + nextLimit\n" +
        "end\n" +
        "return reserved"
    )
    List<TimeJobInfo> reserveJobs(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("limit") int limit,
            @BindArg("now") long now,
            @BindArg("ttl") long ttl);

    /**
     * Jobs in the ready queue are scored/sorted by when they should run. A ready job
     * is considered expired if its score/scheduled run time is less than equal to the
     * expirationTimeInMillis. As a result expirationTimeInMillis should be set to
     * now - (the threshold past which a job sitting waiting to be reserved is considered expired).
     *
     * Jobs in the running queue are sorted by their time to live (ttl). A running job
     * is considered expired if the current time is past the ttl. So expirationTimeInMillis
     * should be set to now.
     * @param queue queue name.
     * @param expirationTimeInMillis boundary in milliseconds for expired jobs.
     * @return list of jobs that were removed.
     */
    @Mapper(TimeJobInfoListMapper.class)
    @Query(
        "local expiredJobs = redis.call('ZRANGEBYSCORE', $queue$, 0, $expirationTimeInMillis$, 'WITHSCORES')\n" +
        "redis.call('ZREMRANGEBYSCORE', $queue$, 0, $expirationTimeInMillis$)\n" +
        "return expiredJobs"
    )
    List<TimeJobInfo> removeExpiredJobs(@BindKey("queue") String queue,
                                        @BindArg("expirationTimeInMillis") Long expirationTimeInMillis);

    /**
     * Deleting a job deletes it from the scheduler in its entirety.
     * @param readyQueue the ready queue name
     * @param runningQueue the running queue name
     * @param job the string representation of a job.
     * @return 1 if the scheduled job was deleted or 0 otherwise.
     */
    @Query(
        "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
        "local deletedFromRunningQueue = redis.call('ZREM', $runningQueue$, $job$)\n" +
        "local deletedFromReadyAndRunningQueue = redis.call('ZREM', $readyAndRunningQueue$, $job$)\n" +

        "if deletedFromReadyQueue == 0 and deletedFromRunningQueue == 0 and deletedFromReadyAndRunningQueue == 0 then\n" +
        "   return 0\n" +
        "else\n" +
        "   return 1\n" +
        "end"
    )
    int deleteJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("readyAndRunningQueue") String readyAndRunningQueue,
            @BindArg("job") String job);

    /**
     * Ack indicates we are done with a reserved job that is in the running queue.
     * Another instance of that job might already be queued up in the ready queue waiting
     * to be reserved.
     * @param runningQueue the running queue name
     * @param job the string representation of a scheduled job.
     * @return 1 if the job is currently in the running queue or 0 otherwise.
     */
    @Query(
        "local removedFromRunning = redis.call('ZREM', $runningQueue$, $job$)\n" +
        "local readyAndRunningJobScore = redis.call('ZSCORE', $readyAndRunningQueue$, $job$)\n"+
        "if readyAndRunningJobScore then\n" +
        "  redis.call('ZREM', $readyAndRunningQueue$, $job$)\n" +
        "  redis.call('ZADD', $readyQueue$, readyAndRunningJobScore, $job$)\n" +
        "end\n" +
        "return removedFromRunning"
    )
    int ackJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("readyAndRunningQueue") String readyAndRunningQueue,
            @BindArg("job") String job);

    @Query(
        "local inQueue = redis.call('ZSCORE', $queue$, $job$)\n" +
        "if inQueue then\n" +
        "   return 1\n" +
        "else\n" +
        "   return 0\n" +
        "end"
    )
    int inQueue(
            @BindKey("queue") String queue,
            @BindArg("job") String job);

    @Query(
            "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "local deletedFromReadyAndRunningQueue = redis.call('ZREM', $readyAndRunningQueue$, $job$)\n" +
            "if deletedFromReadyQueue == 0 and deletedFromReadyAndRunningQueue == 0 then\n" +
            "   return 0\n" +
            "else\n" +
            "   return 1\n" +
            "end"
    )
    int deleteJobFromReady(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("readyAndRunningQueue") String readyAndRunningQueue,
            @BindArg("job") String job);

    @Query(
            "local readyCount = redis.call('ZCARD', $readyQueue$)\n"+
            "return readyCount + redis.call('ZCARD', $readyAndRunningQueue$)"
    )
    int getReadyJobCount(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("readyAndRunningQueue") String readyAndRunningQueue
                        );
}
