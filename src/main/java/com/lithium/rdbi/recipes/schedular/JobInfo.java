package com.lithium.rdbi.recipes.schedular;

import org.joda.time.Instant;

public class JobInfo {

    private final String jobStr;
    private final Instant ttl;
    private final Instant ttr;
    private final JobState state;

    JobInfo(JobState state, String jobStr, Instant ttl, Instant ttr) {
        this.state = state;
        this.jobStr = jobStr;
        this.ttl = ttl;
        this.ttr = ttr;
    }

    public JobState getState() {
        return state;
    }

    public String getJobStr() {
        return jobStr;
    }

    public Instant getTtl() {
        return ttl;
    }

    public Instant getTtr() {
        return ttr;
    }
}
