package org.apache.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

public abstract class RedisSourceSplit implements SourceSplit, Serializable {

    protected final String splitId;

    protected RedisSourceSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisScanSourceSplit that = (RedisScanSourceSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
