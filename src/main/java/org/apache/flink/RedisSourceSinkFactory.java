package org.apache.flink;


import org.apache.flink.common.RedisOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.sink.RedisDynamicTableSink;
import org.apache.flink.source.RedisDynamicTableSource;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class RedisSourceSinkFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {



    private ReadableConfig options;
    public RedisSourceSinkFactory(){}

    public RedisSourceSinkFactory(ReadableConfig options){
        this.options = options;
    }


    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        options = helper.getOptions();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<Column> columns = schema.getColumns();
        ArrayList<String> columnNames = new ArrayList<>();
        columns.forEach(column -> columnNames.add(column.getName()));
        List<String> primaryKey = schema.getPrimaryKey().get().getColumns();
        return new RedisDynamicTableSource(options,columnNames,primaryKey);

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<Column> columns = schema.getColumns();
        ArrayList<String> columnNames = new ArrayList<>();
        columns.forEach(column -> columnNames.add(column.getName()));
        List<String> primaryKey = schema.getPrimaryKey().get().getColumns();
        ReadableConfig options = helper.getOptions();
        return new RedisDynamicTableSink(options,columnNames,primaryKey);
    }



    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.PASSWORD);
        options.add(RedisOptions.KEY);
        options.add(RedisOptions.MODE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(RedisOptions.SINGLE_HOST);
        options.add(RedisOptions.SINGLE_PORT);
        options.add(RedisOptions.CLUSTER_NODES);
        options.add(RedisOptions.FIELD);
        options.add(RedisOptions.PRIMARY_KEY);
        options.add(RedisOptions.CURSOR);
        options.add(RedisOptions.EXPIRE);
        options.add(RedisOptions.COMMAND);
        options.add(RedisOptions.START);
        options.add(RedisOptions.END);
        options.add(RedisOptions.CONNECTION_MAX_TOTAL);
        options.add(RedisOptions.CONNECTION_MAX_IDLE);
        options.add(RedisOptions.CONNECTION_TEST_WHILE_IDLE);
        options.add(RedisOptions.CONNECTION_TEST_ON_BORROW);
        options.add(RedisOptions.CONNECTION_TEST_ON_RETURN);
        options.add(RedisOptions.CONNECTION_TIMEOUT_MS);
        options.add(RedisOptions.TTL_SEC);
        options.add(RedisOptions.LOOKUP_ADDITIONAL_KEY);
        options.add(RedisOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(RedisOptions.LOOKUP_CACHE_TTL_SEC);

        return options;
    }


}
