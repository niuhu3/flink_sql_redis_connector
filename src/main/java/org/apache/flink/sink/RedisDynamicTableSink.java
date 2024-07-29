package org.apache.flink.sink;

import org.apache.flink.common.RedisOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Objects;

public class RedisDynamicTableSink implements DynamicTableSink {

    private final ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;

    public RedisDynamicTableSink(ReadableConfig options, List<String> primaryKey, List<String> columns) {
        this.options = Preconditions.checkNotNull(options);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);
        this.columns = Preconditions.checkNotNull(columns);

    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {

        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        RedisSink<RowData> redisSink = new RedisSink<>(options, primaryKey, columns);
        Integer parallelism = options.get(RedisOptions.SINK_PARALLELISM);

        return SinkV2Provider.of(redisSink,parallelism);
    }

    @Override
    public RedisDynamicTableSink copy() {
        return new RedisDynamicTableSink(options, primaryKey, columns);
    }

    @Override
    public String asSummaryString() {
        return "redis sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisDynamicTableSink that = (RedisDynamicTableSink) o;
        return Objects.equals(options, that.options) &&
                Objects.equals(primaryKey, that.primaryKey) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(options, primaryKey, columns);
    }
}



