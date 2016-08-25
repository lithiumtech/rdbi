package com.lithium.dbi.rdbi.recipes.cache;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

public class CallbackResult<ValueType> {
    private final Optional<ValueType> value;
    private final Exception error;

    public CallbackResult(final ValueType value) {
        this.error = null;
        this.value = Optional.fromNullable(value);
    }

    public CallbackResult(final Exception ex) {
        this.value = Optional.absent();
        this.error = ex;
    }

    public CallbackResult() {
        this.value = Optional.absent();
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
            return value.orNull();
        }
    }
}
