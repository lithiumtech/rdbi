package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.Callback;
import com.lithium.dbi.rdbi.Handle;
import com.lithium.dbi.rdbi.RDBI;
import org.joda.time.Instant;

import java.util.List;

public class PriorityBasedJobScheduler extends AbstractJobScheduler {

    /**
     * @param rdbi the rdbi driver
     * @param redisPrefixKey the prefix key for the job system. All keys the job system uses will have the prefix redisPrefixKey
     */
    public PriorityBasedJobScheduler(RDBI rdbi, String redisPrefixKey) {
        super(rdbi, redisPrefixKey);
    }

    /**
     * Add a new job to the TimeBasedJobScheduler
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @param jobStr A string representation of the job to be scheduled
     * @param priority The "priority" of the job. The higher the number, the higher the priority.
     * @return true if the job was successfully scheduled
     */
    public boolean schedule(final String tube, final String jobStr, final double priority) {
        return rdbi.withHandle(new Callback<Boolean>() {
            @Override
            public Boolean run(Handle handle) {
                handle.jedis().zadd(getReadyQueue(tube), priority, jobStr);
                return true;
            }
        });
    }

    /**
     * Reserve multiple jobs at once.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @param timeToReserveMillis The duration of the reservation in milliseconds, after which time the jobs become available to be put back into the ready queue again if not deleted first.
     * @param maxNumberOfJobs The maximum number of jobs to reserve
     * @return The list of jobs that have been reserved
     */
    @Override
    public List<JobInfo> reserveMulti(final String tube, final long timeToReserveMillis, final int maxNumberOfJobs) {
        try (Handle handle = rdbi.open()) {
            return handle
                    .attach(JobSchedulerDAO.class)
                    .reserveJobs(
                            getReadyQueue(tube),
                            getRunningQueue(tube),
                            maxNumberOfJobs,
                            Long.MIN_VALUE,
                            Long.MAX_VALUE,
                            Instant.now().getMillis() + timeToReserveMillis);
        }
    }

    /**
     * Move any jobs whose reservations have expired back into the ready queue.
     *
     * @param tube Used in conjunction with the redisPrefixKey (constructor) to make up the full redis key name.
     * @return The list of jobs that had expired and that have been already moved back into the ready queue.
     */
    public List<JobInfo> requeueExpired(final String tube, final double newScore) {
        try (Handle handle = rdbi.open()) {
            return handle
                    .attach(JobSchedulerDAO.class)
                    .requeueExpiredJobs(getReadyQueue(tube),
                                        getRunningQueue(tube),
                                        Instant.now().getMillis(),
                                        newScore);
        }
    }

}
