package io.tiledb.spark;

import io.tiledb.java.api.Datatype;
import java.io.Serializable;

public class IntegerDimRange implements Serializable {

    private final String name;
    private final Integer idx;
    private final Datatype dtype;
    private final Long start;
    private final Long end;

    public IntegerDimRange(String name, Integer idx, Datatype dtype, Long start, Long end) {
        this.name = name;
        this.idx = idx;
        this.dtype = dtype;
        this.start = start
        this.end = end;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Long getSpan() {
        return end - start;
    }
}
