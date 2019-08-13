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
            "local perChannelLimit = tonumber($perChannelLimit$)\n" +
            "local perChannelTracking = redis.call('GET', $perChannelTracking$)\n" +
            "local function perChannelLimitReached(running) \n" +
            "  return perChannelTracking and perChannelLimit > 0 and running >= perChannelLimit\n" +
            "end\n" +
            "if runningLimit > 0 then\n" +
            "  local nowRunning = redis.call('ZCARD', $runningQueue$)\n" +
            "  if nextLimit + nowRunning > runningLimit then\n" +
            "    return reserved\n" +
            "  end\n" +
            "end\n" +
            "local channelCount = redis.call('LLEN', $multiChannelCircularBuffer$)\n" +
            "if channelCount == 0 then\n" +
            "  return reserved\n" +
            "end\n" +
            "for chanIdx = 1, channelCount do\n" +
            "  local nextChannel = redis.call('RPOPLPUSH', $multiChannelCircularBuffer$, $multiChannelCircularBuffer$)\n" +
            "  local readyQueue = nextChannel .. \":ready_queue\"\n" +
            "  local runningCount = nextChannel .. \":running_count\"\n" +
            "  local pausedTube = nextChannel .. \":paused\"\n" +
            "  local isPaused = redis.call('GET', pausedTube) \n" +
            "  local hasReady = redis.call('ZCARD', readyQueue)\n" +
            "  local runningForChannel = redis.call('GET', runningCount)\n" +
            "  if runningForChannel then\n" +
            "    runningForChannel = tonumber(runningForChannel)\n" +
            "  else \n" +
            "    runningForChannel = 0\n" +
            "  end\n" +
            "  if hasReady == 0 then\n" +
            //   as a result of RPOPLPUSH call above we know our channel is at the head of the list
            "    redis.call('LPOP', $multiChannelCircularBuffer$)\n" +
            "    redis.call('SREM', $multiChannelSet$, nextChannel)\n" +
            "  end\n" +
            "  local nextOffset = 0\n" +
            "  while nextLimit > 0 and not isPaused and hasReady > 0 and not perChannelLimitReached(runningForChannel) do\n" +
            "    local jobs = redis.call('ZRANGEBYSCORE', readyQueue, 0, $now$, 'WITHSCORES', 'LIMIT', nextOffset, nextLimit)\n" +
            "    if next(jobs) == nil then\n" +
            "      break\n" +
            "    end\n" +
            "    for i=1,#jobs,2 do\n" +
            "      if perChannelLimitReached(runningForChannel) then\n" +
            "        break\n" +
            "      end\n" +
            "      local inRunningQueue = redis.call('ZSCORE', $runningQueue$, jobs[i])\n" +
            "      if not inRunningQueue then\n" +
            "        reserved[reservedIndex] = jobs[i]\n" +
            "        reserved[reservedIndex + 1] = jobs[i + 1]\n" +
            "        redis.call('ZREM', readyQueue, reserved[reservedIndex])\n" +
            "        redis.call('ZADD', $runningQueue$, $ttl$, reserved[reservedIndex])\n" +
            "        if perChannelTracking then \n" +
            "          redis.call('INCR', runningCount)\n" +
            "          runningForChannel = runningForChannel + 1\n" +
            "        end\n" +
            "        reservedIndex = reservedIndex + 2\n" +
            "        nextLimit = nextLimit - 1\n" +
            "        local hasReady = redis.call('ZCARD', readyQueue)\n" +
            "        if hasReady == 0 then\n" +
            //         as a result of RPOPLPUSH call above we know our channel is at the head of the list
            "          redis.call('LPOP', $multiChannelCircularBuffer$)\n" +
            "          redis.call('SREM', $multiChannelSet$, nextChannel)\n" +
            "        end\n" +
            "      end\n" +
            "    end\n" +
            "    nextOffset = nextOffset + nextLimit\n" +
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
            @BindKey("perChannelTracking") String perChannelTracking,
            @BindArg("limit") int limit,
            @BindArg("runningLimit") int runningLimit,
            @BindArg("perChannelLimit") int perChannelLimit,
            @BindArg("now") long now,
            @BindArg("ttl") long ttl);

    /**
     * Will attempt to reserve up to $limit jobs for only 1 channel
     * This method does not honor any per-channel limitations at present time
     */
    @Mapper(TimeJobInfoListMapper.class)
    @Query(
            "local reserved = {}\n" +
            "local isPaused = redis.call('GET', $pausedTube$) \n" +
            "if isPaused then\n" +
            "    return reserved\n" +
            "end\n" +
            "local nextLimit = tonumber($limit$)\n" +
            "local runningLimit = tonumber($runningLimit$)\n" +
            "if runningLimit > 0 then\n" +
            "  local nowRunning = redis.call('ZCARD', $runningQueue$)\n" +
            "  if nextLimit + nowRunning > runningLimit then\n" +
            "    return reserved\n" +
            "  end\n" +
            "end\n" +
            "local perChannelTracking = redis.call('GET', $perChannelTracking$)\n" +
            "local reservedIndex = 1\n" +
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
            "          if perChannelTracking then \n" +
            "            redis.call('INCR', $runningCount$)\n" +
            "          end\n" +
            "          reservedIndex = reservedIndex + 2\n" +
            "          nextLimit = nextLimit - 1\n" +
            "      end\n" +
            "   end\n" +
            "   nextOffset = nextOffset + nextLimit\n" +
            "end\n" +
            "return reserved"
    )
    List<TimeJobInfo> reserveJobsForChannel(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindKey("perChannelTracking") String perChannelTracking,
            @BindKey("pausedTube") String pausedTube,
            @BindKey("runningCount") String runningCountKey,
            @BindArg("limit") int limit,
            @BindArg("runningLimit") int runningLimit,
            @BindArg("now") long now,
            @BindArg("ttl") long ttl);

    @Mapper(TimeJobInfoListMapper.class)
    @Query(
            "local expiredJobs = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $expirationTimeInMillis$, 'WITHSCORES')\n" +
            "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $expirationTimeInMillis$)\n" +
            "return expiredJobs"
    )
    List<TimeJobInfo> removeExpiredRunningJobs(@BindKey("runningQueue") String runningQueue,
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
            "local removed = redis.call('ZREM', $runningQueue$, $job$)" +
            "local running = redis.call('GET', $runningCount$)\n" +
            "if running and tonumber(running) > 0 then \n" +
            "  local remaining = redis.call('DECRBY', $runningCount$, removed)\n" +
            "  if tonumber(remaining) == 0 then\n" +
            "    redis.call('DEL', $runningCount$)\n" +
            "  end\n" +
            "end\n" +
            "if running and tonumber(running) < 0 then \n" +
            "  redis.call('SET', $runningCount$, 0)\n" +
            "end\n" +
            "return removed"
    )
    int ackJob(
            @BindKey("runningQueue") String runningQueue,
            @BindKey("runningCount") String runningCountKey,
            @BindArg("job") String job);


    @Query(
            "local increment = redis.call('ZINCRBY', $runningQueue$, $ttlIncrement$, $job$)\n" +
            "if increment == $ttlIncrement$ then \n" +
            "  redis.call('ZREM', $runningQueue$, $job$)\n" +
            "  return 0\n" +
            "else\n" +
            "  return 1\n" +
            "end"
    )
    int incrementTTL(@BindKey("runningQueue") String runningQueue,
                     @BindArg("ttlIncrement") long ttlIncrement,
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

    @Query( "local removedFromRunning = redis.call('ZREM', $runningQueue$, $job$)\n" +
            "local removedFromReady = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "if removedFromReady > 0 then\n" +
            "  local hasReady = redis.call('ZCARD', $readyQueue$)\n" +
            "  if hasReady == 0 then\n" +
            "     redis.call('LREM', $multiChannelCircularBuffer$, 1, $channelPrefix$)\n" +
            "     redis.call('SREM', $multiChannelSet$, $channelPrefix$)\n" +
            "  end\n" +
            "end\n" +
            "if removedFromRunning > 0 then\n" +
            "  local running = redis.call('GET', $runningCount$)\n" +
            "  if running and tonumber(running) > 0 then \n" +
            "    local remaining = redis.call('DECRBY', $runningCount$, removedFromRunning)\n" +
            "    if tonumber(remaining) == 0 then\n" +
            "      redis.call('DEL', $runningCount$)\n" +
            "    end\n" +
            "  end\n" +
            "  if running and tonumber(running) < 0 then \n" +
            "    redis.call('DEL', $runningCount$)\n" +
            "  end\n" +
            "end\n" +
            "if removedFromReady == 0 and removedFromRunning == 0 then\n" +
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
            @BindKey("runningCount") String runningCountKey,
            @BindArg("channelPrefix") String channelPrefix,
            @BindArg("job") String job);

    @Query(
            "local running = redis.call('GET', $runningCount$)\n" +
            "if running and tonumber(running) > 0 then \n" +
            "  local remaining = redis.call('DECR', $runningCount$)\n" +
            "  if tonumber(remaining) == 0 then\n" +
            "    redis.call('DEL', $runningCount$)\n" +
            "  end\n" +
            "  return 1\n" +
            "end\n" +
            "if running and tonumber(running) < 0 then\n" +
            "  redis.call('DEL', $runningCount$)\n" +
            "end\n" +
            "return 0\n"
    )
    long decrementRunningCount(@BindKey("runningCount") String runningCountKey);
}
