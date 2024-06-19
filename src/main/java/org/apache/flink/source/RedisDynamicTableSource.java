package org.apache.flink.source;


import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.util.Preconditions;

import java.util.List;

public class RedisDynamicTableSource implements ScanTableSource {

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;


    public RedisDynamicTableSource(ReadableConfig options, List<String> columns, List<String> primaryKey) {
        this.options = Preconditions.checkNotNull(options);
        this.columns = Preconditions.checkNotNull(columns);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);

    }



    @Override
    public DynamicTableSource copy() {

        return new RedisDynamicTableSource(this.options, this.columns, this.primaryKey);
    }




    @Override
    public String asSummaryString() {
        return "redis table source";
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        RedisSourceFunction redisSourceFunction = new RedisSourceFunction(this.options, this.columns, this.primaryKey);
        return SourceFunctionProvider.of(redisSourceFunction,false);
    }
}
