package org.apache.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class RedisSink <IN> implements Sink<IN> {

    private static final long serialVersionUID = 1L;

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;

    public RedisSink(ReadableConfig options,
                     List<String> primaryKey,
                     List<String> columns){

        this.options = Preconditions.checkNotNull(options);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);
        this.columns = Preconditions.checkNotNull(columns);
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        Integer batchSize = options.get(RedisOptions.BUFFER_FLUSH_MAX_ROWS);
        Long batchIntervalMs = options.get(RedisOptions.BUFFER_FLUSH_INTERVAL).toMillis();
        Integer maxRetries = options.get(RedisOptions.MAX_ATTEMPTS);
        Long retryIntervalMs = options.get(RedisOptions.LOOKUP_RETRY_INTERVAL).toMillis();
        DeliveryGuarantee deliveryGuarantee = options.get(RedisOptions.DELIVERY_GUARANTEE);

        RedisWriteOptions redisWriteOptions = new RedisWriteOptions(batchSize,batchIntervalMs,maxRetries,retryIntervalMs, deliveryGuarantee);

        return new RedisWriter<>(options,primaryKey,columns,context,redisWriteOptions);
    }
}
