package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

public class CallbackResult<ValueType> {
    private final Optional<ValueType> value;
    private final Exception error;

    public CallbackResult(final ValueType value) {
        this.error = null;
        this.value = Optional.of(value);
    }

    public CallbackResult(final Exception ex) {
        this.value = Optional.absent();
        this.error = ex;
    }

    public Optional<ValueType> getValue() {
        return value;
    }

    public Exception getError() {
        return error;
    }

    public ValueType getOrThrow() throws Exception {
        if (value.isPresent()) {
            return value.get();
        } else {
            throw error;
        }
    }

    public ValueType getOrThrowUnchecked() {
        if (value.isPresent()) {
            return value.get();
        } else {
            throw Throwables.propagate(error);
        }
    }
}
