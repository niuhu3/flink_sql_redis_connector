package org.apache.flink.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class RedisWriteOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;
    private final long retryIntervalMs;
    private final DeliveryGuarantee deliveryGuarantee;

    public RedisWriteOptions(
            int batchSize,
            long batchIntervalMs,
            int maxRetries,
            long retryIntervalMs,
            DeliveryGuarantee deliveryGuarantee) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
        this.deliveryGuarantee = deliveryGuarantee;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RedisWriteOptions that = (RedisWriteOptions) o;
        return batchSize == that.batchSize
                && batchIntervalMs == that.batchIntervalMs
                && maxRetries == that.maxRetries
                && retryIntervalMs == that.retryIntervalMs
                && deliveryGuarantee == that.deliveryGuarantee;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                batchSize, batchIntervalMs, maxRetries, retryIntervalMs, deliveryGuarantee);
    }

    public boolean flushOnCheckpoint() {
        return getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE;
    }
}





