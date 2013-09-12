package com.lithium.rdbi.recipes.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.lithium.rdbi.*;
import org.joda.time.Instant;

import java.util.List;

//CR: Need a class-level javadoc explaining what this does and pointing reader to appropriate readme docs for more detail.
public class ExclusiveJobScheduler {

    //CR: Are you trying to say that this implements the same Beanstalk Protocol?
    //https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt

    private final RDBI rdbi;
    private final String prefix;

    //CR: Can we move the DAO out to it's own standalone .java file?  Also, interfaces don't need visibility modifiers on methods.
    static interface DAO {
        @Query(
            "local readyJobScore = redis.call('ZSCORE', $readyQueue$, $jobStr$)\n" +
            "local runningJobScore = redis.call('ZSCORE', $runningQueue$, $jobStr$)\n" +
            "if not readyJobScore and not runningJobScore then\n" +
            "   redis.call('ZADD', $readyQueue$, $ttl$, $jobStr$)\n" +
            "   return 1\n" +
            "else\n" +
            "   return 0\n" +
            "end"
        )
        //CR: the "Str" suffix on the job parameter seems redundant
        //CR: using Long object type instead of long - do you really need, want or handle the null case?  Applies throughout this DAO.
        public int schedule(
                @BindKey("readyQueue") String readyQueue,
                @BindKey("runningQueue") String runningQueue,
                @BindArg("jobStr") String jobStr,
                @BindArg("ttl") Long ttlInMillis);

        @Mapper(JobInfoListMapper.class)
        @Query(
            "local jobs = redis.call('ZRANGEBYSCORE', $readyQueue$, 0, $now$, 'WITHSCORES', 'LIMIT', 0, $limit$)\n" +
            "if next(jobs) == nil then\n" +
            "    return nil\n" +  //CR: Will returning null cause your mapper to barf ?
            "end\n" +
            "local job = jobs[1]\n" +  //CR: What's the point of this local var?
            "for i=1,2*#jobs,2 do\n" +
            "    redis.call('ZREM', $readyQueue$, jobs[i])\n" +         //note expensive: in order to support "limit", we have to loop the delete
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
        @Query(
            "local lateJobs = redis.call('ZRANGEBYSCORE', $runningQueue$, 0, $now$, 'WITHSCORES')\n" +  //CR: the word "late" in this var is confusing to me. What makes it "late"?
            "redis.call('ZREMRANGEBYSCORE', $runningQueue$, 0, $now$)\n" +
            "return lateJobs"
        )
        //CR: This could be a little more descriptive.  e.g. "removeRunningJobs"
        public List<JobInfo> cull(@BindKey("runningQueue") String runningQueue, @BindArg("now") Long now);

        @Query(
            "local deletedFromReadyQueue = redis.call('ZREM', $readyQueue$, $job$)\n" +
            "local deletedFromRunningQueue = redis.call('ZREM', $runningQueue$, $job$)\n" +
            "if not deleteFromReadyQueue and not deletedFromRunningQueue then\n" +
            "   return 0\n" +
            "else\n" +
            "   return 1\n" +
            "end"
        )
        //CR: A more descriptive method name would be sweet.
        public int clear(
            @BindKey("readyQueue") String readyQueue,
            @BindKey("runningQueue") String runningQueue,
            @BindArg("job") String job);
    }

    //CR: need javadoc on all public methods in this file since it will be used by anyone invoking this recipe.  Prefix is an ambiguous parameter name too.
    public ExclusiveJobScheduler(RDBI rdbi, String prefix) {
        this.rdbi = rdbi;
        this.prefix = prefix;
    }

    //CR: wtf is a "tube"?
    public boolean schedule(final String tube, final String jobStr, final int ttlInMillis) {
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                return 1 == handle.attach(DAO.class).schedule(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        jobStr,
                        Instant.now().getMillis() + ttlInMillis);
            }
        });
    }

    //CR: provide a reserveSingle and reserveMultiple API whereby the reserveSingle is just a special case of reserveMultiple
    //CR: the very subtle distinction between ttl and ttr will likely slip by many people.  Javadoc may help - but something to consider.
    public List<JobInfo> reserve(final String tube, final long ttrInMillis) {
        return rdbi.withHandle(new Callback<List<JobInfo>>() {
            @Override
            public List<JobInfo> run(Handle handle) {
                return handle.attach(DAO.class).reserve(
                        getReadyQueue(tube),
                        getRunningQueue(tube),
                        1,
                        Instant.now().getMillis(),
                        Instant.now().getMillis() + ttrInMillis);
            }
        });
    }

    //CR: unused method - means to me you should add a test for it.
    //CR: two different styles of using RDBI in this class.  Some using .withHandle and some using .open/.close.  Pick one and stick to it.
    public boolean clear(final String tube, String jobStr) {
        Handle handle = rdbi.open();
        try {
            return 1 == handle.attach(DAO.class).clear(getReadyQueue(tube), getRunningQueue(tube), jobStr);
        } finally {
            handle.close();
        }
    }

    public List<JobInfo> cull(String tube) {
        Handle handle = rdbi.open();
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
        rdbi.withHandle(new Callback<Void>() {
            @Override
            public Void run(Handle handle) {
                handle.jedis().del(prefix + tube + ":ready_queue",
                                   prefix + tube + ":running_queue");
                return null;
            }
        });
    }
}
