package com.lithium.rdbi;

class MethodContext {

    private final String sha1;
    private final RedisResultMapper mapper;

    public MethodContext(String sha1, RedisResultMapper mapper) {
        this.sha1 = sha1;
        this.mapper = mapper;
    }

    public RedisResultMapper getMapper() {
        return mapper;
    }

    public String getSHA1() {
        return sha1;
    }
}
