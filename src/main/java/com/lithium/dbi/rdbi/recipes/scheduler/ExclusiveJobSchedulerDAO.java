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
        "if not readyJobScore and not runningJobScore and isPaused ~= 'true' then\n" +
        "   redis.call('ZADD', $readyQueue$, $ttl$, $jobStr$)\n" +
        "   return 1\n" +
        "else\n" +
        "   return 0\n" +
        "end"
    )
    public int scheduleJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("jobStr") String job,
            @BindArg("ttl") long ttlInMillis);

    @Mapper(JobInfoListMapper.class)
    @Query(
        "local isPaused = redis.call('GET', $pausedTube$) \n" +
        "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
        "if isPaused == 'true' or next(jobs) == nil then\n" +
        "    return nil\n" +
        "end\n" +
        "for i=1,2*#jobs,2 do\n" +
        "    redis.call('ZREM', $readyQueue$, jobs[i])\n" + //Note: in order to support "limit", we have to loop the delete, perhaps not have limit, 1 at a time?
        "    redis.call('ZADD', $runningQueue$, $ttr$, jobs[i])\n" +
        "end\n" +
        "return jobs"
    )
    public List<JobInfo> reserveJobs(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindArg("limit") int limit,
            @BindArg("now") long now,
            @BindArg("ttr") long ttr);

    @Mapper(JobInfoListMapper.class)
    @Query(
        "local expiredJob = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $now$, 'WITHSCORES')\n" +
        "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $now$)\n" +
        "return expiredJob"
    )
    public List<JobInfo> removeExpiredJobs(@BindKey("runningQueue") String runningQueue, @BindArg("now") Long now);

    @Query(
        "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
        "local deletedFromRunningQueue = redis.call('ZREM', $runningQueue$, $job$)\n" +
        "if not deletedFromReadyQueue and not deletedFromRunningQueue then\n" +
        "   return 0\n" +
        "else\n" +
        "   return 1\n" +
        "end"
    )
    public int deleteJob(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);
}
