package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.BindArg;
import com.lithium.dbi.rdbi.BindKey;
import com.lithium.dbi.rdbi.Mapper;
import com.lithium.dbi.rdbi.Query;

import java.util.List;

public interface MultiChannelSchedulerDAO {

    @Query(
            "local isPaused = redis.call('GET', $pausedTube$) \n" +
            "if isPaused then\n" +
            "    return 0\n" +
            "end\n" +
            "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
            "if readyJobScore then\n" +
            "  return 0\n" +
            "end\n" +
            "redis.call('ZADD', $readyQueue$, $runInMillis$, $jobStr$)\n" +
            "local inCircularBuffer = redis.call('SISMEMBER', $multiChannelSet$, $channelPrefix$)\n" +
            "if inCircularBuffer == 0 then\n" +
            "   redis.call('SADD', $multiChannelSet$, $channelPrefix$)\n" +
            "   redis.call('LPUSH', $multiChannelCircularBuffer$, $channelPrefix$)\n" + //NOTE this guarantees fairness at the cost of latency (perhaps); we do not update timestamp if it exists; this is pure round robin, round robining based on last-execution-time
            "end\n" +
            "return 1\n"
    )
    int scheduleJob(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindKey("multiChannelSet") String multiChannelSet,
            @BindKey("readyQueue") String readyQueue,
            @BindKey("pausedTube") String pausedTube,
            @BindKey("channelPrefix") String channelPrefix,
            @BindArg("jobStr") String job,
            @BindArg("runInMillis") long runInMillis);

    /**
     * Will attempt to reserve up to $limit jobs across multiple channels.
     *
     * Note that if multiple jobs are requested and found in one channel, they will
     * all be fulfilled within that channel, not necessarily following the round robin
     * expectation.
     *
     */
    @Mapper(TimeJobInfoListMapper.class)
    @Query(
            "local reservedIndex = 1\n" +
            "local nextLimit = tonumber($limit$)\n" +
            "local reserved = {}\n" +
            "local runningLimit = tonumber($runningLimit$)\n" +
            "if runningLimit > 0 then\n" +
            "  local nowRunning = redis.call('ZCARD', $runningQueue$)\n" +
            "  if nextLimit + nowRunning > runningLimit then\n" +
            "    return reserved" +
            "  end\n" +
            "end\n" +
            "local channelCount = redis.call('LLEN', $multiChannelCircularBuffer$)\n" +
            "if channelCount == 0 then\n" +
            "  return reserved\n" +
            "end\n" +
            "for chanIdx = 1, channelCount do\n" +
            "  local nextChannel = redis.call('RPOPLPUSH', $multiChannelCircularBuffer$, $multiChannelCircularBuffer$)\n" +
            "  local readyQueue = nextChannel .. \":ready_queue\"\n" +
            "  local pausedTube = nextChannel .. \":paused\"\n" +
            "  local isPaused = redis.call('GET', pausedTube) \n" +
            "  local nextOffset = 0\n" +
            "  while nextLimit > 0 and not isPaused do\n" +
            "     local jobs = redis.call('ZRANGEBYSCORE', readyQueue, 0, $now$, 'WITHSCORES', 'LIMIT', nextOffset, nextLimit)\n" +
            "     if next(jobs) == nil then\n" +
            "         break\n" +
            "     end\n" +
            "     for i=1,#jobs,2 do\n" +
            "        local inRunningQueue = redis.call('ZSCORE', $runningQueue$, jobs[i])\n" +
            "        if not inRunningQueue then\n" +
            "            reserved[reservedIndex] = jobs[i]\n" +
            "            reserved[reservedIndex + 1] = jobs[i + 1]\n" +
            "            redis.call('ZREM', readyQueue, reserved[reservedIndex])\n" +
            "            redis.call('ZADD', $runningQueue$, $ttl$, reserved[reservedIndex])\n" +
            "            reservedIndex = reservedIndex + 2\n" +
            "            nextLimit = nextLimit - 1\n" +
            "            local hasReady = redis.call('ZCARD', readyQueue)\n" +
            "            if hasReady == 0 then\n" +
            //             as a result of RPOPLPUSH call above we know our channel is at the head of the list
            "              redis.call('LPOP', $multiChannelCircularBuffer$)\n" +
            "              redis.call('SREM', $multiChannelSet$, nextChannel)\n" +
            "            end\n" +
            "        end\n" +
            "     end\n" +
            "     nextOffset = nextOffset + nextLimit\n" +
            "  end\n" +
            // return early if we have enough jobs
            "  if nextLimit == 0 then\n" +
            "    return reserved\n" +
            "  end\n" +
            "end\n" +
            "return reserved"
    )
    List<TimeJobInfo> reserveJobs(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindKey("multiChannelSet") String multiChannelSet,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("limit") int limit,
            @BindArg("runningLimit") int runningLimit,
            @BindArg("now") long now,
            @BindArg("ttl") long ttl);

    @Mapper(TimeJobInfoListMapper.class)
    @Query(
            "local expiredJobs = redis.call('ZRANGEBYSCORE', $queue$, 0, $expirationTimeInMillis$, 'WITHSCORES')\n" +
            "redis.call('ZREMRANGEBYSCORE', $queue$, 0, $expirationTimeInMillis$)\n" +
            "return expiredJobs"
    )
    List<TimeJobInfo> removeExpiredJobs(@BindKey("queue") String queue,
                                        @BindArg("expirationTimeInMillis") Long expirationTimeInMillis);

    @Mapper(TimeJobInfoListMapper.class)
    @Query(
            "local channelCount = redis.call('LLEN', $multiChannelCircularBuffer$)\n" +
            "local expired = {}\n" +
            "local expiredIndex = 1\n" +
            "for chanIdx = 1, channelCount do\n" +
            "  local nextChannel = redis.call('RPOPLPUSH', $multiChannelCircularBuffer$, $multiChannelCircularBuffer$)\n" +
            "  local readyQueue = nextChannel .. \":ready_queue\"\n" +
            "  local expiredJobs = redis.call('ZRANGEBYSCORE', readyQueue, 0, $expirationTimeInMillis$, 'WITHSCORES')\n" +
            "  if #expiredJobs > 0 then\n" +
            "    redis.call('ZREMRANGEBYSCORE', readyQueue, 0, $expirationTimeInMillis$)\n" +
            "    local hasReady = redis.call('ZCARD', readyQueue)\n" +
            "    if hasReady == 0 then\n" + // need to clean up, no more ready jobs for this type
            "       redis.call('LPOP', $multiChannelCircularBuffer$)\n" +
            "       redis.call('SREM', $multiChannelSet$, nextChannel)\n" +
            "    end\n" +
            "    for j = 1, #expiredJobs do\n" +
            "      expired[expiredIndex] = expiredJobs[j]\n" +
            "      expiredIndex = expiredIndex + 1\n" +
            "    end\n" +
            "  end\n" +
            "end\n" +
            "return expired"
    )
    List<TimeJobInfo> removeAllExpiredReadyJobs(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindKey("multiChannelSet") String multiChannelSet,
            @BindArg("expirationTimeInMillis") Long expirationTimeInMillis);

    /**
     * Ack indicates we are done with a reserved job that is in the running queue.
     * Another instance of that job might already be queued up in the ready queue waiting
     * to be reserved.
     * @param runningQueue the running queue name
     * @param job the string representation of a scheduled job.
     * @return 1 if the job is currently in the running queue or 0 otherwise.
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

    @Query(
            "local channels = redis.call('LRANGE',$multiChannelCircularBuffer$, 0, -1)\n" +
            "local count = 0\n" +
            "for i, channel in ipairs(channels) do \n" +
            "  local readyQueue = channel .. \":ready_queue\"\n" +
            "  count = count + redis.call('ZCOUNT', readyQueue, 0, $currentTimeMillis$)\n" +
            "end\n" +
            "return count"
    )
    long getAllReadyJobCount(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindArg("currentTimeMillis") long currentTimeMillis);

    @Query(
            "local removed = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "if removed > 0 then\n" +
            "  local hasReady = redis.call('ZCARD', $readyQueue$)\n" +
            "  if hasReady == 0 then\n" +
            "     redis.call('LREM', $multiChannelCircularBuffer$, 1, $channelPrefix$)\n" +
            "     redis.call('SREM', $multiChannelSet$, $channelPrefix$)\n" +
            "  end\n" +
            "end\n" +
            "return removed"
    )
    int deleteJobFromReady(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindKey("multiChannelSet") String multiChannelSet,
            @BindKey("readyQueue") String readyQueue,
            @BindArg("channelPrefix") String channelPrefix,
            @BindArg("job") String job);

    @Query(
            "local removedFromRunning = redis.call('ZREM', $runningQueue$, $job$)\n" +
            "local removed = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "if removed > 0 then\n" +
            "  local hasReady = redis.call('ZCARD', $readyQueue$)\n" +
            "  if hasReady == 0 then\n" +
            "     redis.call('LREM', $multiChannelCircularBuffer$, 1, $channelPrefix$)\n" +
            "     redis.call('SREM', $multiChannelSet$, $channelPrefix$)\n" +
            "  end\n" +
            "end\n" +
            "if removed == 0 and removedFromRunning == 0 then\n" +
            "  return 0\n" +
            "else\n" +
            "  return 1\n" +
            "end"
    )
    int deleteJob(
            @BindKey("multiChannelCircularBuffer") String multiChannelCircularBuffer,
            @BindKey("multiChannelSet") String multiChannelSet,
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("channelPrefix") String channelPrefix,
            @BindArg("job") String job);
}
