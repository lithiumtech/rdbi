package com.lithium.dbi.rdbi.recipes.scheduler;

public class JobInfo {

    protected final String jobStr;
    protected final double jobScore;

    public JobInfo(String jobStr, double jobScore) {
        this.jobStr = jobStr;
        this.jobScore = jobScore;
    }

    public String getJobStr() {
        return jobStr;
    }

    public double getJobScore() {
        return jobScore;
    }

    @Override
    public String toString() {
        return "JobInfo{" +
                "jobStr='" + jobStr + '\'' +
                ", jobScore=" + jobScore +
                '}';
    }
}
