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
     */
    @Query(
        "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "if not readyJobScore and not isPaused then\n" +
        "   redis.call('ZADD', $readyQueue$, $runInMillis$, $jobStr$)\n" +
        "   return 1\n" +
        "else\n" +
        "   return 0\n" +
        "end"
    )
    int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("jobStr") String job,
            @BindArg("runInMillis") long runInMillis);

    /**
     * When reserving jobs reserve jobs that are currently in ready queue but not in the running queue.
     * If the job is in running queue need to wait till the running instance of the job has completed before
     * we allow another reservation of the job from the ready queue.
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
        "   for i=1,2*#jobs,2 do\n" +
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
     */
    @Query(
        "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
        "local deletedFromRunningQueue = redis.call('ZREM', $runningQueue$, $job$)\n" +
        "if deletedFromReadyQueue == 0 and deletedFromRunningQueue == 0 then\n" +
        "   return 0\n" +
        "else\n" +
        "   return 1\n" +
        "end"
    )
    int deleteJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);

    /**
     * Ack indicates we are done with a reserved job that is in the running queue.
     * Another instance of that job might already be queued up in the ready queue waiting
     * to be reserved.
     */
    @Query(
        "return redis.call('ZREM', $runningQueue$, $job$)"
    )
    int ackJob(
            @BindKey("runningQueue") String runningQueue,
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

}
