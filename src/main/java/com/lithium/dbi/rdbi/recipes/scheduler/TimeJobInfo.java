package com.lithium.dbi.rdbi.recipes.scheduler;

import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Extends {@link JobInfo}, further interpreting the job's score as its scheduled time of execution.
 */
public class TimeJobInfo extends JobInfo {

    private final Instant time;

    TimeJobInfo(String jobStr, double jobScore) {
        super(jobStr, jobScore);
        time = new Instant((long) jobScore);
    }

    public Instant getTime() {
        return time;
    }

    @Override
    public String toString() {
        return "JobInfo{" +
                "jobStr='" + jobStr + '\'' +
                ", time=" + time +
                '}';
    }

    public static List<TimeJobInfo> from(@Nullable List<JobInfo> jobInfos) {
        if (jobInfos == null) {
            return null;
        }
        return jobInfos.stream()
                       .map(info -> new TimeJobInfo(info.getJobStr(), info.getJobScore()))
                       .collect(Collectors.toList());
    }
}
