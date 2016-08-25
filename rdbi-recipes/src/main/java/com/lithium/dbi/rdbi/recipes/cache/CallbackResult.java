package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.Throwables;

import java.util.Optional;

public class CallbackResult<ValueType> {
    private final Optional<ValueType> value;
    private final Exception error;

    public CallbackResult(final ValueType value) {
        this.error = null;
        this.value = Optional.ofNullable(value);
    }

    public CallbackResult(final Exception ex) {
        this.value = Optional.empty();
        this.error = ex;
    }

    public CallbackResult() {
        this.value = Optional.empty();
        this.error = null;
    }

    public Optional<ValueType> getValue() {
        return value;
    }

    public Exception getError() {
        return error;
    }

    public ValueType getOrThrowUnchecked() {
        if (error != null) {
            throw Throwables.propagate(error);
        } else {
            return value.orElse(null);
        }
    }
}
