package com.lithium.rdbi.recipes.scheduler;

public enum JobState {

    DELAYED(1),
    READY(2),
    RESERVED(3),
    TIMEDOUT(4),
    INTERNALERROR(5);

    private final int state;

    JobState(int state) {
        this.state = state;
    }

    public int getValue() {
        return state;
    }
}
