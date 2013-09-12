package com.lithium.rdbi.recipes.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.lithium.rdbi.*;
import org.joda.time.Instant;

import java.util.List;

public class ExclusiveJobScheduler {

    //https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt

    private final RDBI rdbi;
    private final String prefix;

    static interface DAO {
        @RedisQuery(
            "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
            "local runningJobScore = redis.call('ZSCORE', $runningQueue$, $jobStr$)\n" +
            "if not readyJobScore and not runningJobScore then\n" +
            "   redis.call('ZADD', $readyQueue$, $ttl$, $jobStr$)\n" +
            "   return 1\n" +
            "else\n" +
            "   return 0\n" +
            "end"
        )
        public int schedule(
                @BindKey("readyQueue") String readyQueue,
                @BindKey("runningQueue") String runningQueue,
                @BindArg("jobStr") String jobStr,
                @BindArg("ttl") Long ttlInMillis);

        @Mapper(JobInfoListMapper.class)
        @RedisQuery(
            "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
            "if next(jobs) == nil then\n" +
            "    return nil\n" +
            "end\n" +
            "local job = jobs[1]\n" +
            "for i=1,2*#jobs,2 do\n" +
            "    redis.call('ZREM', $readyQueue$, jobs[i])\n" +         //note expensive: inorder to support "limit", we have to loop the delete
            "    redis.call('ZADD', $runningQueue$, $ttr$, jobs[i])\n" +
            "end\n" +
            "return jobs"
        )
        public List<JobInfo> reserve(
                @BindKey("readyQueue") String readyQueue,
                @BindKey("runningQueue") String runningQueue,
                @BindArg("limit") Integer limit,
                @BindArg("now") Long now,
                @BindArg("ttr") Long ttr);

        @Mapper(JobInfoListMapper.class)
        @RedisQuery(
            "local lateJobs = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $now$, 'WITHSCORES')\n" +
            "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $now$)\n" +
            "return lateJobs"
        )
        public List<JobInfo> cull(@BindKey("runningQueue") String runningQueue, @BindArg("now") Long now);

        @RedisQuery(
            "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "local deletedFromRunningQueue = redis.call('ZREM', $runningQueue$, $job$)\n" +
            "if not deleteFromReadyQueue and not deletedFromRunningQueue then\n" +
            "   return 0\n" +
            "else\n" +
            "   return 1\n" +
            "end"
        )
        public int clear(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);
    }

    public ExclusiveJobScheduler(RDBI rdbi, String prefix) {
        this.rdbi = rdbi;
        this.prefix = prefix;
    }

    public boolean schedule(final String tube, final String jobStr, final int ttlInMillis) {
        return rdbi.withHandle(new JedisCallback<Boolean>() {
            @Override
            public Boolean run(JedisHandle handle) {
                return 1 == handle.attach(DAO.class).schedule(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        jobStr,
                        Instant.now().getMillis() + ttlInMillis);
            }
        });
    }

    public List<JobInfo> reserve(final String tube, final long ttrInMillis) {
        return rdbi.withHandle(new JedisCallback<List<JobInfo>>() {
            @Override
            public List<JobInfo> run(JedisHandle handle) {
                return handle.attach(DAO.class).reserve(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        1,
                        Instant.now().getMillis(),
                        Instant.now().getMillis() + ttrInMillis);
            }
        });
    }

    public boolean clear(final String tube, String jobStr) {
        JedisHandle handle = rdbi.open();
        try {
            return 1 == handle.attach(DAO.class).clear(getReadyQueue(tube), getRunningQueue(tube), jobStr);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> cull(String tube) {
        JedisHandle handle = rdbi.open();
        try {
            return handle.attach(DAO.class).cull(getRunningQueue(tube), Instant.now().getMillis());
        } finally {
            handle.close();
        }
    }

    private String getRunningQueue(String tube) {
        return prefix + tube + ":running_queue";
    }

    private String getReadyQueue(String tube) {
        return prefix + tube + ":ready_queue";
    }

    @VisibleForTesting
    void nukeForTest(final String tube) {
        rdbi.withHandle(new JedisCallback<Void>() {
            @Override
            public Void run(JedisHandle handle) {
                handle.jedis().del(prefix + tube + ":ready_queue", prefix + tube + ":running_queue");
                return null;
            }
        });
    }
}
