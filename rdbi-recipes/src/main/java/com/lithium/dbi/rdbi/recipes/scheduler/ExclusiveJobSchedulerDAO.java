package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface ExclusiveJobSchedulerDAO {

    @Query(
        "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
        "local runningJobScore = redis.call('ZSCORE', $runningQueue$, $jobStr$)\n" +
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "if not readyJobScore and not runningJobScore and not isPaused then\n" +
        "   redis.call('ZADD', $readyQueue$, $ttl$, $jobStr$)\n" +
        "   return 1\n" +
        "else\n" +
        "   return 0\n" +
        "end"
    )
    int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("jobStr") String job,
            @BindArg("ttl") long ttlInMillis);

    @Mapper(TimeJobInfoListMapper.class)
    @Query(
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
        "if isPaused or next(jobs) == nil then\n" +
        "    return nil\n" +
        "end\n" +
        "for i=1,#jobs,2 do\n" +
        "    redis.call('ZREM', $readyQueue$, jobs[i])\n" + //Note: in order to support "limit", we have to loop the delete, perhaps not have limit, 1 at a time?
        "    redis.call('ZADD', $runningQueue$, $ttr$, jobs[i])\n" +
        "end\n" +
        "return jobs"
    )
    List<TimeJobInfo> reserveJobs(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("limit") int limit,
            @BindArg("now") long now,
            @BindArg("ttr") long ttr);

    /**
     * Jobs in the ready queue are scored/sorted by when they should run. A ready job
     * is considered expired if its score/scheduled run time is less than equal to the
     * expirationTimeInMillis. As a result expirationTimeInMillis should be set to
     * now - (the threshold past which a job sitting waiting to be reserved is considered expired).
     *
     * Jobs in the running queue are sorted by their time to live (ttl). A running job
     * is considered expired if the current time is past the ttl. So expirationTimeInMillis
     * should be set to now.
     * @param queue queue name
     * @param expirationTimeInMillis tthe age in milliseconds beyond which a job is considered expired.
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
}
