package com.lithium.dbi.rdbi;

import java.util.Set;

class LuaContext {

    private final String renderedLuaString;
    private final Set<Integer> keys;

    public LuaContext(String renderedLuaString, Set<Integer> keys) {
        this.renderedLuaString = renderedLuaString;
        this.keys = keys;
    }

    public String getRenderedLuaString() {
        return renderedLuaString;
    }

    public boolean isKey(int key) {
        return keys.contains(key);
    }

    @Override
    public String toString() {
        return "RenderedResult{" +
                "renderedLuaString='" + renderedLuaString + '\'' +
                ", keys=" + keys +
                '}';
    }
}
