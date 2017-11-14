package com.lithium.dbi.rdbi.recipes.channel;

import com.google.common.collect.Lists;

import java.util.List;

public class GetBulkResult {

    //ordered list
    private final List<List<String>> messages;

    private final List<Long> depths; //should be called lastmessageseen

    public GetBulkResult(List<List<String>> messages, List<Long> depths) {
        this.messages = messages;
        this.depths = depths;
    }

    public GetBulkResult() {
        this.messages = Lists.newArrayList();
        this.depths = Lists.newArrayList();
    }

    public List<List<String>> getMessages() {
        return messages;
    }

    public List<Long> getDepths() {
        return depths;
    }
}
