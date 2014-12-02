package com.lithium.dbi.rdbi.recipes.scheduler;

public class JobInfo {

    private final String jobStr;
    private final double jobScore;

    JobInfo(String jobStr, double jobScore) {
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
