package com.lithium.rdbi.recipes.scheduler;

public enum JobState {
    DELAYED,
    READY,
    RESERVED,
    TIMEDOUT,
    INTERNALERROR
}
