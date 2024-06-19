package org.apache.flink.sink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class RedisDynamicTableSink implements DynamicTableSink {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisDynamicTableSink.class);

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;

    public RedisDynamicTableSink(ReadableConfig options, List<String> columns, List<String> primaryKey) {
        this.options = Preconditions.checkNotNull(options);
        this.columns = Preconditions.checkNotNull(columns);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();

    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RedisSinkFunction myRedisSinkFunction = new RedisSinkFunction(this.options,this.columns,this.primaryKey);
        return SinkFunctionProvider.of(myRedisSinkFunction);

    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(this.options,this.columns,this.primaryKey);
    }

    @Override
    public String asSummaryString() {
        return "redis table sink";
    }
}

