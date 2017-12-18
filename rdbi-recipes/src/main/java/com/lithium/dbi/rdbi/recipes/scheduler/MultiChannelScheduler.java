package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.*;
import org.joda.time.Instant;

import java.util.List;

public class MultiChannelScheduler {
    private final RDBI rdbi;
    private final String prefix;

    public MultiChannelScheduler(RDBI rdbi, String redisPrefixKey) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
    }

    /**
     *
     * This is a pure round robin scheduler. If the next channel up doesn't have data it returns immediately
     * instead of moving on to the next one.
     *
     * NOTE this is no longer compatible with redis cluster because we do not specify the keys upfront
     */
    public interface MultiChannelSchedulerDAO {

        @Query(
                "local isPaused = redis.call('GET', $pausedTube$) \n" +
                "if isPaused then\n" +
                "    return 0\n" +
                "end\n" +
                "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
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

        //We could add an extra loop in order to go through all the companies if they're empty, but we should check
        //that we've run one full circle OR we can remove things that have nothing in the ready set; I don't know how this
        //will interact if we fix the n^2logn problem here either
        @Mapper(TimeJobInfoListMapper.class)
        @Query(
                "local nextChannel = redis.call('RPOPLPUSH', $multiChannelCircularBuffer$, $multiChannelCircularBuffer$)\n" +
                "if not nextChannel then\n" +
                "    return {}\n" +
                "end\n" +
                "local readyQueue = nextChannel .. \":ready_queue\"\n" +
                "local runningQueue = nextChannel .. \":running_queue\"\n" +
                "local pausedTube = nextChannel .. \":paused\"\n" +
                "local reserved = {}\n" +
                "local isPaused = redis.call('GET', pausedTube) \n" +
                "if isPaused then\n" +
                "    return reserved\n" +
                "end\n" +
                "local reservedIndex = 1\n" +
                "local nextLimit = tonumber($limit$)\n" +
                "local nextOffset = 0\n" +
                "while nextLimit > 0 do\n" +
                "   local jobs = redis.call('ZRANGEBYSCORE', readyQueue, 0, $now$, 'WITHSCORES', 'LIMIT', nextOffset, nextLimit)\n" +
                "   if next(jobs) == nil then\n" +
                "       return reserved\n" +
                "   end\n" +
                "   for i=1,#jobs,2 do\n" +
                "      local inRunningQueue = redis.call('ZSCORE', runningQueue, jobs[i])\n" +
                "      if not inRunningQueue then\n" +
                "          reserved[reservedIndex] = jobs[i]\n" +
                "          reserved[reservedIndex + 1] = jobs[i + 1]\n" +
                "          redis.call('ZREM', readyQueue, reserved[reservedIndex])\n" +
                "          redis.call('ZADD', runningQueue, $ttl$, reserved[reservedIndex])\n" +
                "          reservedIndex = reservedIndex + 2\n" +
                "          nextLimit = nextLimit - 1\n" +
                "      end\n" +
                "   end\n" +
                "   nextOffset = nextOffset + nextLimit\n" +
                "end\n" +
                "return reserved"
        )
        List<TimeJobInfo> reserveJobs(
                @BindKey("multiChannelCircularBuffer") String multiChannelQueue,
                @BindArg("limit") int limit,
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

    }

    public boolean schedule(final String group, final String tube, final String jobStr, final int runInMillis) {

        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                    .scheduleJob(
                            getMultiChannelCircularBuffer(group),
                            getMultiChannelSet(group),
                            getReadyQueue(group, tube),
                            getPaused(group, tube),
                            getTubePrefix(group, tube),
                            jobStr,
                            Instant.now().getMillis() + runInMillis);
        }
    }

    public List<TimeJobInfo> reserveMulti(final String group, final long considerExpiredAfterMillis, final int maxNumberOfJobs) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class).reserveJobs(
                    getMultiChannelCircularBuffer(group),
                    maxNumberOfJobs,
                    Instant.now().getMillis(),
                    Instant.now().getMillis() + considerExpiredAfterMillis);
        }
    }

    public List<String> getAllChannels(final String group) {
        try (Handle handle = rdbi.open()) {
            return handle.jedis().lrange(getMultiChannelCircularBuffer(group), 0, -1);
        }
    }

    public boolean ackJob(final String group, final String tube, final String jobStr) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(StateDedupedJobSchedulerDAO.class)
                    .ackJob(getRunningQueue(group, tube), jobStr);
        }
    }


    public List<TimeJobInfo> removeExpiredRunningJobs(String group, String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(StateDedupedJobSchedulerDAO.class)
                    .removeExpiredJobs(getRunningQueue(group, tube), Instant.now().getMillis());
        }
    }

    private String getMultiChannelCircularBuffer(String group) {
        return prefix + ":multichannel:" + group + ":circular_buffer";
    }
    private String getMultiChannelSet(String group) {
        return prefix + ":multichannel:" + group + ":set";
    }

    private String getTubePrefix(String group, String tube) {
        return prefix + ":tube:" + group + ":" + tube;
    }

    private String getReadyQueue(String group, String tube) {
        return prefix + ":tube:" + group + ":" + tube + ":ready_queue";
    }

    private String getRunningQueue(String group, String tube) {
        return prefix + ":tube:" + group + ":" + tube + ":running_queue";
    }

    private String getPaused(String group, String tube){
        return prefix + ":tube:" + group + ":" + tube + ":paused";
    }
}
