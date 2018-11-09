package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.primitives.Ints;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * This is similar to {@link StateDedupedJobScheduler}, except that it includes a separate "channel"
 * dimension and attempts to maintain fairness across channels among jobs in a particular "tube".
 * <p>
 * Includes support for methods from {@link StateDedupedJobScheduler} and it's abstract hierarchy,
 * however does not extend from that tree because some methods now require the channel parameter
 * <p>
 * Definitions:
 * "tube" here means the same as it means in other scheduler variations. A tube corresponds
 * to a specific set of sorted sets in redis and represents a grouping of a particular type of
 * job. All calls must reference a tube. Jobs are scheduled for a particular tube, and when
 * {@link #reserveMulti(String, long, int)} is called, they are pulled from that referenced tube.
 * <p>
 * "channel" here is a new dimension, and a single tube can hold jobs for multiple channels (implemented
 * as distinct sorted sets in redis). jobs for a single channel will be reserved in FIFO order
 * for that particular channel, but jobs in the same tube for a different channel will be
 * reserved by round-robin through all active channels. Thus a glut of jobs in one channel
 * should not adversely affect other channels. The possibility, of course, still exists that
 * one channel can delay itself....
 * <p>
 * it may prove useful to create a MultiChannelScheduler.ForChannel that extends from the {@link AbstractDedupJobScheduler} hierarchy
 * // TODO This doesn't yet incorporate concepts from https://github.com/lithiumtech/rdbi/commit/6bbf2eeb49b87b71655f24fa9b797300b37b6797, that will be tackled separately
 */
public class MultiChannelScheduler {
    private final RDBI rdbi;
    private final String prefix;
    private final LongSupplier clock;

    public MultiChannelScheduler(RDBI rdbi, String redisPrefixKey, LongSupplier clock) {
        this.rdbi = rdbi;
        this.prefix = redisPrefixKey;
        this.clock = clock;
    }

    public MultiChannelScheduler(RDBI rdbi, String redisPrefixKey) {
        this(rdbi, redisPrefixKey, System::currentTimeMillis);
    }

    /**
     * see {@link AbstractDedupJobScheduler#schedule(String, String, int)}
     *
     * @return true if the job was scheduled.
     * false indicates the job already exists in the ready queue.
     */
    public boolean schedule(String channel, String tube, final String job, final int runInMillis) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                              .scheduleJob(
                                      getMultiChannelCircularBuffer(tube),
                                      getMultiChannelSet(tube),
                                      getReadyQueue(channel, tube),
                                      getPausedKey(channel, tube),
                                      getTubePrefix(channel, tube),
                                      job,
                                      clock.getAsLong() + runInMillis);
        }
    }

    /**
     * see {@link AbstractDedupJobScheduler#reserveMulti(String, long, int)}
     *
     */
    public List<TimeJobInfo> reserveMulti(String tube, long considerExpiredAfterMillis, final int maxNumberOfJobs) {
        return reserveMulti(tube, considerExpiredAfterMillis, maxNumberOfJobs, 0);
    }

    /**
     * attempt to reserve 1 or more jobs while also specifying a global running limit on jobs for this tube.
     *
     * if the current # of running jobs + maxNumberOfJobs attempted to reserve is &gt; runningLimit, no jobs
     * will be reserved.
     *
     * see also {@link AbstractDedupJobScheduler#reserveMulti(String, long, int)}
     *
     * @param tube job group. we will only grab ready jobs from this group.
     * @param considerExpiredAfterMillis if jobs haven't been deleted after being reserved for this many millis, consider them expired.
     * @param maxNumberOfJobs number of jobs to reserve.
     * @param runningLimit if &gt; 0, a limit of jobs we want to allow running for this particular tube type. If &lt;= 0, no limit will be enforced.
     * @return list of jobs reserved (now considered "running",) or empty list if none.
     */
    public List<TimeJobInfo> reserveMulti(String tube, long considerExpiredAfterMillis, final int maxNumberOfJobs, final int runningLimit) {
        return reserveMulti(tube, considerExpiredAfterMillis, maxNumberOfJobs, runningLimit, 0);
    }

    /**
     * attempt to reserve 1 or more jobs while also specifying a global running limit on jobs for this tube.
     *
     * if the current # of running jobs + maxNumberOfJobs attempted to reserve is &gt; runningLimit, no jobs
     * will be reserved.
     *
     * see also {@link AbstractDedupJobScheduler#reserveMulti(String, long, int)}
     *
     * @param tube job group. we will only grab ready jobs from this group.
     * @param considerExpiredAfterMillis if jobs haven't been deleted after being reserved for this many millis, consider them expired.
     * @param maxNumberOfJobs number of jobs to reserve.
     * @param runningLimit if &gt; 0, a limit of jobs we want to allow running for this particular tube type. If &lt;= 0, no limit will be enforced.
     * @param perChannelLimit if &gt; 0, a limit of jobs we want to allow running for any particular channel / tube combination. If &lt;= 0, no limit will be enforced.
     *                        Note that prior to using this, you must have called {@link #enablePerChannelTracking()}, otherwise this parameter will be ignored.
     *                        Before enabling this, all scheduler clients should be upgraded to a version that supports per-channel tracking &amp; limits
     * @return list of jobs reserved (now considered "running",) or empty list if none.
     */
    public List<TimeJobInfo> reserveMulti(String tube, long considerExpiredAfterMillis, final int maxNumberOfJobs, final int runningLimit, final int perChannelLimit) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class).reserveJobs(
                    getMultiChannelCircularBuffer(tube),
                    getMultiChannelSet(tube),
                    getRunningQueue(tube),
                    getPerChannelTrackingEnabled(),
                    maxNumberOfJobs,
                    runningLimit,
                    perChannelLimit,
                    clock.getAsLong(),
                    clock.getAsLong() + considerExpiredAfterMillis);
        }
    }

    public List<TimeJobInfo> reserveMultiForChannel(String channel, String tube, long considerExpiredAfterMillis, final int maxNumberOfJobs, final int runningLimit) {
        try(Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class).reserveJobsForChannel(
                    getReadyQueue(channel, tube),
                    getRunningQueue(tube),
                    getPerChannelTrackingEnabled(),
                    getPausedKey(channel, tube),
                    getRunningCountKey(channel, tube),
                    maxNumberOfJobs,
                    runningLimit,
                    clock.getAsLong(),
                    clock.getAsLong() + considerExpiredAfterMillis);
        }
    }

    public List<String> getAllReadyChannels(final String tube) {
        try (Handle handle = rdbi.open()) {
            // mc buffer holds the prefixes, we have to
            // decompose them to get the channel only
            return handle.jedis().lrange(getMultiChannelCircularBuffer(tube), 0, -1)
                    .stream()
                    // rm our prefix
                    .map(chPrefix -> chPrefix.replaceFirst(prefix + ":", ""))
                    // rm tube suffix
                    .map(channelAndTube -> channelAndTube.replace(":" + tube, ""))
                    .collect(Collectors.toList());
        }
    }

    public long getAllReadyJobCount(String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class)
                         .getAllReadyJobCount(
                                 getMultiChannelCircularBuffer(tube),
                                 clock.getAsLong());
        }
    }

    /**
     * See {@link StateDedupedJobScheduler#ackJob(java.lang.String, java.lang.String)}
     */
    public boolean ackJob(String channel, String tube, String job) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                              .ackJob(getRunningQueue(tube),
                                      getRunningCountKey(channel, tube),
                                      job);
        }
    }

    /**
     * See {@link StateDedupedJobScheduler#removeExpiredRunningJobs(java.lang.String)}
     **/
    public List<TimeJobInfo> removeExpiredRunningJobs(String tube) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class)
                         .removeExpiredJobs(getRunningQueue(tube),
                                            clock.getAsLong());
        }
    }

    /**
     * removes expired ready jobs across all channels
     */
    public List<TimeJobInfo> removeExpiredReadyJobs(String tube, long expirationPeriodInMillis) {
        try (Handle handle = rdbi.open()) {
            return handle.attach(MultiChannelSchedulerDAO.class)
                         .removeAllExpiredReadyJobs(getMultiChannelCircularBuffer(tube),
                                                    getMultiChannelSet(tube),
                                                    clock.getAsLong() - expirationPeriodInMillis);
        }
    }

    /**
     * Deletes a job from either ready or running queue (or both)
     */
    public boolean deleteJob(String channel, String tube, String job) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                              .deleteJob(getMultiChannelCircularBuffer(tube),
                                         getMultiChannelSet(tube),
                                         getReadyQueue(channel, tube),
                                         getRunningQueue(tube),
                                         getRunningCountKey(channel, tube),
                                         getTubePrefix(channel, tube),
                                         job
                                        );
        }

    }

    /**
     * Delete job only from the ready queue
     */
    public boolean deleteJobFromReady(String channel, String tube, String job) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                              .deleteJobFromReady(
                                      getMultiChannelCircularBuffer(tube),
                                      getMultiChannelSet(tube),
                                      getReadyQueue(channel, tube),
                                      getTubePrefix(channel, tube),
                                      job);
        }

    }

    /**
     * This will "pause" the system for the specified tube / channel combo, preventing any new jobs from being scheduled
     * or reserved.
     *
     * @param tube the name of related jobs
     */
    public void pause(String channel, String tube) {
        rdbi.withHandle(handle -> {
            handle.jedis().set(getPausedKey(channel, tube), String.valueOf(clock.getAsLong() / 1000));
            return null;
        });
    }

    public boolean isPaused(String channel, String tube) {
        return rdbi.withHandle(handle -> handle.jedis().get(getPausedKey(channel, tube)) != null);
    }

    /**
     * This returns the value for the pause key. If that value was created through this library
     * it will be a unix timestamp (seconds since the epoch).
     */
    public String getPauseStart(String channel, String tube) {
        return rdbi.withHandle(handle -> handle.jedis().get(getPausedKey(channel, tube)));
    }

    /**
     * This will resume / un-pause the system for the specified tube, allowing jobs to be scheduled and reserved.
     */
    public void resume(String channel, String tube) {
        rdbi.withHandle(handle -> {
            handle.jedis().del(getPausedKey(channel, tube));
            return null;
        });
    }

    public long getReadyJobCount(String channel, String tube) {
        final String queue = getReadyQueue(channel, tube);
        return rdbi.withHandle(handle -> handle.jedis().zcount(queue, 0, clock.getAsLong()));
    }

    public long getRunningJobCount(String tube) {
        final String queue = getRunningQueue(tube);
        return rdbi.withHandle(handle -> handle.jedis().zcard(queue));
    }

    /**
     * returns a list of jobs scheduled with a delay - to run in the future
     */
    public List<TimeJobInfo> peekDelayed(String channel, String tube, int offset, int count) {
        return peekInternal(getReadyQueue(channel, tube), (double) clock.getAsLong(), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekReady(String channel, String tube, int offset, int count) {
        return peekInternal(getReadyQueue(channel, tube), 0.0d, (double) clock.getAsLong(), offset, count);
    }

    public List<TimeJobInfo> peekRunning(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), (double) clock.getAsLong(), Double.MAX_VALUE, offset, count);
    }

    public List<TimeJobInfo> peekExpired(String tube, int offset, int count) {
        return peekInternal(getRunningQueue(tube), 0.0d, (double) clock.getAsLong(), offset, count);
    }

    public Integer getRunningCountForChannel(String channel, String tube) {
       final String key = getRunningCountKey(channel, tube);
       final String count = rdbi.withHandle(h -> h.jedis().get(key));
       return Optional.ofNullable(count)
               .map(Ints::tryParse)
               .orElse(0);
    }

    private List<TimeJobInfo> peekInternal(String queue, Double min, Double max, int offset, int count) {
        try (Handle handle = rdbi.open()) {
            Set<Tuple> tupleSet = handle.jedis().zrangeByScoreWithScores(queue, min, max, offset, count);
            return tupleSet.stream()
                           .map(t -> new TimeJobInfo(t.getElement(), t.getScore()))
                           .collect(Collectors.toList());
        }
    }

    public boolean inReadyQueue(String channel, String tube, String job) {
        return inQueue(getReadyQueue(channel, tube), job);
    }

    public boolean inRunningQueue(String tube, String job) {
        return inQueue(getRunningQueue(tube), job);
    }

    public boolean isPerChannelTrackingEnabled() {
        return rdbi.withHandle(h -> h.jedis().get(getPerChannelTrackingEnabled()) != null);
    }

    /**
     * enables per-channel tracking. This must be called before per channel limits can be honored
     * Before enabling this, all scheduler clients should be upgraded to a version that supports per-channel tracking &amp; limits
     *
     * @return true if tracking was previously disabled, false if the tracking had already been enabled
     */
    public boolean enablePerChannelTracking() {
        return rdbi.withHandle(h -> h.jedis().setnx(getPerChannelTrackingEnabled(), "1") == 1 );
    }


    /**
     * disabled per-channel tracking.
     * @return true if tracking was previously enabled, false if the tracking had already been disabled
     */
    public boolean disablePerChannelTracking() {
        return rdbi.withHandle(h -> h.jedis().del(getPerChannelTrackingEnabled()) != 0 );
    }



    private boolean inQueue(String queueName, String job) {
        try (Handle handle = rdbi.open()) {
            return 1 == handle.attach(MultiChannelSchedulerDAO.class)
                              .inQueue(queueName, job);
        }
    }

    private String getMultiChannelCircularBuffer(String tube) {
        return prefix + ":multichannel:" + tube + ":circular_buffer";
    }

    private String getMultiChannelSet(String tube) {
        return prefix + ":multichannel:" + tube + ":set";
    }

    private String getTubePrefix(String channel, String tube) {
        return prefix + ":" + channel + ":" + tube;
    }

    private String getReadyQueue(String channel, String tube) {
        return getTubePrefix(channel, tube) + ":ready_queue";
    }

    private String getRunningQueue(String tube) {
        return prefix + ":multichannel:" + tube + ":running_queue";
    }

    private String getPausedKey(String channel, String tube) {
        return getTubePrefix(channel, tube) + ":paused";
    }

    private String getRunningCountKey(String channel, String tube) {
        return getTubePrefix(channel, tube) + ":running_count";
    }

    private String getPerChannelTrackingEnabled() {
        return prefix + ":per_channel_tracking_enabled";
    }
}
