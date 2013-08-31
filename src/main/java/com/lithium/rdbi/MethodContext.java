package com.lithium.rdbi;

class MethodContext {

    private final String sha1;
    private final RedisResultMapper mapper;
    private final LuaContext tranformerContext;

    public MethodContext(String sha1, RedisResultMapper mapper, LuaContext tranformerContext) {
        this.sha1 = sha1;
        this.mapper = mapper;
        this.tranformerContext = tranformerContext;
    }

    public RedisResultMapper getMapper() {
        return mapper;
    }

    public String getSha1() {
        return sha1;
    }

    public LuaContext getTranformerContext() {
        return tranformerContext;
    }

    public boolean hasDynamicLists() {
        return tranformerContext != null;
    }
}
