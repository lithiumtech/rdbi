package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.joda.time.Instant;

import javax.annotation.Nullable;
import java.util.List;

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
        return Lists.transform(jobInfos, new Function<JobInfo, TimeJobInfo>() {
            @Override
            public TimeJobInfo apply(JobInfo info) {
                return new TimeJobInfo(info.getJobStr(), info.getJobScore());
            }
        });
    }
}
