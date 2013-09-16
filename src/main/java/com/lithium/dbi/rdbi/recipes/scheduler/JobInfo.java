package com.lithium.dbi.rdbi.recipes.scheduler;

import org.joda.time.Instant;

public class JobInfo {

    private final String jobStr;
    private final Instant time;

    JobInfo(String jobStr, Instant time) {
        this.jobStr = jobStr;
        this.time = time;
    }

    public String getJobStr() {
        return jobStr;
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
}
