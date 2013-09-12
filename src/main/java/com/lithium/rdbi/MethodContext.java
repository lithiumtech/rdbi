package com.lithium.rdbi;

class MethodContext {

    private final String sha1;
    private final ResultMapper mapper;
    private final LuaContext luaContext;

    public MethodContext(String sha1, ResultMapper mapper, LuaContext luaContext) {
        this.sha1 = sha1;
        this.mapper = mapper;
        this.luaContext = luaContext;
    }

    public ResultMapper getMapper() {
        return mapper;
    }

    public String getSha1() {
        return sha1;
    }

    public LuaContext getLuaContext() {
        return luaContext;
    }

    public boolean hasDynamicLists() {
        return luaContext != null;
    }
}
