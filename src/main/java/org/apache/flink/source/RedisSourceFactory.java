package org.apache.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;

import java.util.List;

public class RedisSourceFactory implements ScanTableSource{

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;

    public RedisSourceFactory(ReadableConfig options,
                              List<String> primaryKey,
                              List<String> columns){
        this.options = options;
        this.primaryKey = primaryKey;
        this.columns = columns;
    }

    @Override
    public ChangelogMode getChangelogMode() {

        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        return SourceProvider.of(new RedisSourceFunctionV2(options,Boundedness.BOUNDED,primaryKey,columns));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisSourceFactory(this.options,this.primaryKey,this.columns);
    }

    @Override
    public String asSummaryString() {
        return "redis";
    }
}
