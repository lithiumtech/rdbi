package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.Lists;

import java.util.List;

public class GetResult {

    //ordered list
    private final List<String> messages;

    private final Long depth; //should be called lastmessageseen

    public GetResult(List<String> messages, Long depth) {
        this.messages = messages;
        this.depth = depth;
    }

    public GetResult() {
        this.messages = Lists.newArrayList();
        this.depth = 0L;
    }

    public List<String> getMessages() {
        return messages;
    }

    public Long getDepth() {
        return depth;
    }
}
