package com.lithium.rdbi.recipes.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.lithium.rdbi.JedisCallback;
import com.lithium.rdbi.JedisHandle;
import com.lithium.rdbi.RDBI;
import com.lithium.rdbi.RedisQuery;
import org.joda.time.Instant;

import java.util.List;

public class ExclusiveJobSchedular {

    //https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt

    private final RDBI rdbi;
    private final String prefix;

    static interface DAO {
        @RedisQuery(
            "local ready_queue = KEYS[1]\n" +
            "local running_queue = KEYS[2]\n" +
            "local job_str = ARGV[1]\n" +
            "local job_score = ARGV[2]\n" +
            "local readyq_jobrank = redis.call(\"ZRANK\", ready_queue, job_str)\n" +
            "local runningq_jobrank = redis.call(\"ZRANK\", running_queue, job_str)\n" +
            "if not readyq_jobrank and not runningq_jobrank then\n" +
            "    redis.call(\"ZADD\", ready_queue, job_score, job_str)\n" +
            "    return \"1\"\n" +
            "elseif not runningq_jobrank then\n" +
            "    return \"2\"\n" +
            "elseif not readyq_jobrank then\n" +
            "    return \"3\"\n" +
            "else\n" +
            "    return \"0\"\n" +
            "end"
        )
        public String schedule(List<String> keys, List<String> args);

        @RedisQuery(
            "local ready_queue = KEYS[1]\n" +
            "local running_queue = KEYS[2]\n" +
            "local now_ts = ARGV[1]\n" +
            "local ttr_ts = ARGV[2]\n" +
            "local jobs = redis.call(\"ZRANGEBYSCORE\", ready_queue, 0, now_ts, \"LIMIT\", 0, 1)\n" +
            "if next(jobs) == nil then\n" +
            "    return nil\n" +
            "end\n" +
            "local job = jobs[1]\n" +
            "redis.call(\"ZREM\", ready_queue, job)\n" +
            "redis.call(\"ZADD\", running_queue, ttr_ts, job)\n" +
            "return job\n"
        )
        public String reserve(List<String> keys, List<String> args);
    }

    public ExclusiveJobSchedular(RDBI rdbi, String prefix) {
        this.rdbi = rdbi;
        this.prefix = prefix;
    }

    public void schedule(final String tube, final String jobStr, final int ttlInMillis) {
        rdbi.withHandle(new JedisCallback<Void>() {
            @Override
            public Void run(JedisHandle handle) {
                handle.attach(DAO.class).schedule(
                        ImmutableList.of(prefix + tube + ":ready_queue", prefix + tube + ":running_queue"),
                        ImmutableList.of(jobStr, Long.toString((Instant.now().getMillis() + ttlInMillis))));
                return null;
            }
        });
    }

    public String reserve(final String tube, final long ttrInMillis) {
        return rdbi.withHandle(new JedisCallback<String>() {
            @Override
            public String run(JedisHandle handle) {

                return handle.attach(DAO.class).reserve(
                        ImmutableList.of(prefix + tube + ":ready_queue", prefix + tube + ":running_queue"),
                        ImmutableList.of(Long.toString(Instant.now().getMillis()), Long.toString(Instant.now().getMillis() + ttrInMillis)));
            }
        });
    }

    public JobInfo clear(final String tube, String jobStr) {
        return null;
    }

    public List<JobInfo> cull(final String tube) {
        return null;
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
