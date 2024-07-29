package org.apache.flink.source;

import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.*;

public class RedisSourceSplitReader implements SplitReader<RowData, RedisScanSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceSplitReader.class);

    private boolean closed = false;
    private boolean finished = false;
    private Jedis jedis;
    private RedisScanSourceSplit currentSplit;
    private RedisSourceReaderContext readerContext;
    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;
    private JedisCluster jedisCluster;
    private String value;
    private String field;
    private String[] fields;
    private String cursor;
    private Integer start;
    private Integer end;
    private String[] keySplit;
    private static int position = 1;
    private GenericRowData rowData;

    public RedisSourceSplitReader(ReadableConfig options,
                                  RedisSourceReaderContext readerContext,
                                  List<String> primaryKey,
                                  List<String> columns) {

        this.options = options;
        this.readerContext =readerContext;
        this.primaryKey = primaryKey;
        this.columns = columns;
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        if (closed) {
            throw new IllegalStateException("Cannot fetch records from a closed split reader");
        }

        RecordsBySplits.Builder<RowData> builder = new RecordsBySplits.Builder<>();

        // Return when no split registered to this reader.
        if (currentSplit == null) {
            return builder.build();
        }

        List<RowData> rows = readRedisData(options);

        try {
            for (int i = 0; i < rows.size(); i++) {
                if (i != rows.size()) {
                    builder.add(currentSplit, rows.get(i));
                    readerContext.getReadCount().incrementAndGet();
                    if (readerContext.getReadCount().get() >= rows.size()) {
                        builder.addFinishedSplit(currentSplit.splitId());
                        finished = true;
                        break;
                    }
                } else {
                    builder.addFinishedSplit(currentSplit.splitId());
                    finished = true;
                    break;
                }
            }
            return builder.build();
        } catch (Exception e) {
            throw new IOException("Scan records form Redis failed", e);
        } finally {
            if (finished) {

               if(jedis != null){
                   jedis.close();
               }

               if(jedisCluster != null){
                   jedisCluster.close();
               }

            }
        }


    }

    private List<RowData> readRedisData(ReadableConfig options) {

        List<RowData> rows = new ArrayList<>();

        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password,"password is null,please set value for password");
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key,"key is null,please set value for key");
        String[] keyArr = key.split(RedisSplitSymbol.CLUSTER_NODES_SPLIT);
        String command = options.get(RedisOptions.COMMAND);
        
        // judge if command is redis set data command and stop method
        List<String> sourceCommand = Arrays.asList(RedisCommandOptions.SET,
                RedisCommandOptions.HSET, RedisCommandOptions.HMSET,
                RedisCommandOptions.LPUSH,
                RedisCommandOptions.RPUSH,
                RedisCommandOptions.SADD);
        if(sourceCommand.contains(command.toUpperCase())){ return rows;}

        Preconditions.checkNotNull(command,"command is null,please set value for command");
        String mode = options.get(RedisOptions.MODE);
        Preconditions.checkNotNull(command,"mode is null,please set value for mode");
        Integer maxIdle = options.get(RedisOptions.CONNECTION_MAX_IDLE);
        Integer maxTotal = options.get(RedisOptions.CONNECTION_MAX_TOTAL);
        Integer maxWaitMills = options.get(RedisOptions.CONNECTION_MAX_WAIT_MILLS);

        Boolean testOnBorrow = options.get(RedisOptions.CONNECTION_TEST_ON_BORROW);
        Boolean testOnReturn = options.get(RedisOptions.CONNECTION_TEST_ON_RETURN);
        Boolean testWhileIdle = options.get(RedisOptions.CONNECTION_TEST_WHILE_IDLE);


        if(mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())){

            String host = options.get(RedisOptions.SINGLE_HOST);
            Integer port = options.get(RedisOptions.SINGLE_PORT);
            jedis= RedisUtil.getSingleJedis(mode, host, port, maxTotal,
                    maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);
            jedis.auth(password);



            switch (command.toUpperCase()){
                case RedisCommandOptions.GET:
                    value = jedis.get(key);
                    rowData = new GenericRowData(2);
                    rowData.setField(0, BinaryStringData.fromString(key));
                    rowData.setField(1,BinaryStringData.fromString(value));
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.HGET:
                    field = options.get(RedisOptions.FIELD);
                    value = jedis.hget(key, field);
                    rowData = new GenericRowData(3);
                    keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                    for (int i = 0; i < primaryKey.size(); i++) {
                        rowData.setField(i,BinaryStringData.fromString(keyArr[2 * primaryKey.size()]));
                    }
                    rowData.setField(primaryKey.size(),BinaryStringData.fromString(value));
                    rows.add(rowData);
                    break;


                case RedisCommandOptions.HGETALL:
                    if (keyArr.length > 1){
                        for (String str : keyArr) {
                            rowData = new GenericRowData(columns.size());
                            keySplit = str.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                            }

                            for (int i = primaryKey.size(); i < columns.size(); i++) {
                                String value = jedis.hget(str, columns.get(i));
                                rowData.setField(i,BinaryStringData.fromString(value));
                            }
                            rows.add(rowData);
                            break;

                        }

                    }else if(key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT).length == (primaryKey.size() * 2 + 1)){
                        rowData = new GenericRowData(columns.size());
                        keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                        for (int i = 0; i < primaryKey.size(); i++) {
                            rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                        }

                        for (int i = primaryKey.size(); i < columns.size(); i++) {
                            String value = jedis.hget(key, columns.get(i));
                            rowData.setField(i,BinaryStringData.fromString(value));
                        }

                        rows.add(rowData);
                        break;


                    }else{
                        //Fuzzy matching ,gets the data of the entire table
                        String fuzzyKey = new StringBuffer(key).append("*").toString();
                        Set<String> keys = jedis.keys(fuzzyKey);
                        for (String keyStr : keys) {
                            keySplit = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            rowData = new GenericRowData(columns.size());
                            for (int i = 0; i < primaryKey.size(); i++) {
                                rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                            }

                            for (int i = primaryKey.size(); i < columns.size(); i++) {
                                String value = jedis.hget(keyStr, columns.get(i));
                                rowData.setField(i,BinaryStringData.fromString(value));
                            }

                            rows.add(rowData);



                        }
                    }

                    return rows;


                case RedisCommandOptions.HSCAN:
                    cursor = options.get(RedisOptions.CURSOR);
                    ScanResult<Map.Entry<String, String>> entries = jedis.hscan(key, cursor);
                    List<Map.Entry<String, String>> result = entries.getResult();
                    keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                    rowData = new GenericRowData(columns.size());
                    for (int i = 0; i < primaryKey.size(); i++) {
                        rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                    }

                    position = primaryKey.size();
                    for (int i = 0; i < result.size(); i++) {
                        value = result.get(i).getValue();
                        rowData.setField(position,BinaryStringData.fromString(value));
                        position++;
                    }
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.LRANGE:
                    start = options.get(RedisOptions.START);
                    end = options.get(RedisOptions.END);
                    List<String> list = jedis.lrange(key, start, end);
                    rowData = new GenericRowData(list.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    list.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.SMEMBERS:
                    Set<String> smembers = jedis.smembers(key);
                    rowData = new GenericRowData(smembers.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    smembers.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.ZRANGE:
                    start = options.get(RedisOptions.START);
                    end = options.get(RedisOptions.END);
                    Set<String> sets = jedis.zrange(key, start, end);
                    rowData = new GenericRowData(sets.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    sets.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;


                default:
                    LOG.error("Cannot process such data type: {}", command);
                    break;
            }

            if(!command.toUpperCase().equals(RedisCommandOptions.HGETALL)){
                rows.add(rowData);
                return rows;

            }



        }else if(mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())){
            String nodes = options.get(RedisOptions.CLUSTER_NODES);
            String[] hostAndPorts = nodes.split(RedisSplitSymbol.CLUSTER_NODES_SPLIT);
            String[] host = new String[hostAndPorts.length];
            int[] port = new int[hostAndPorts.length];

            for (int i = 0; i < hostAndPorts.length; i++) {
                String[] splits = hostAndPorts[i].split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                host[i] = splits[0];
                port[i] = Integer.parseInt(splits[1]);
            }
            Integer connTimeOut = options.get(RedisOptions.CONNECTION_TIMEOUT_MS);
            Integer soTimeOut = options.get(RedisOptions.SO_TIMEOUT_MS);
            Integer maxAttempts = options.get(RedisOptions.MAX_ATTEMPTS);

            jedisCluster = RedisUtil.getJedisCluster(mode, host, password, port, maxTotal,
                    maxIdle, maxWaitMills, connTimeOut, soTimeOut, maxAttempts, testOnBorrow, testOnReturn, testWhileIdle);

            switch (command.toUpperCase()){
                case RedisCommandOptions.GET:
                    value = jedisCluster.get(key);
                    rowData = new GenericRowData(2);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    rowData.setField(1,BinaryStringData.fromString(value));
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.HGET:
                    field = options.get(RedisOptions.FIELD);
                    value = jedisCluster.hget(key, field);
                    rowData = new GenericRowData(3);
                    keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                    for (int i = 0; i < primaryKey.size(); i++) {
                        rowData.setField(i,BinaryStringData.fromString(keyArr[2 * primaryKey.size()]));
                    }
                    rowData.setField(primaryKey.size(),BinaryStringData.fromString(value));
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.HGETALL:
                    if (keyArr.length > 1){
                        for (String str : keyArr) {
                            rowData = new GenericRowData(columns.size());
                            keySplit = str.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                            }

                            for (int i = primaryKey.size(); i < columns.size(); i++) {
                                String value = jedisCluster.hget(str, columns.get(i));
                                rowData.setField(i,BinaryStringData.fromString(value));
                            }
                            rows.add(rowData);


                        }

                    }else if(key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT).length == (primaryKey.size() * 2 + 1)){
                        rowData = new GenericRowData(columns.size());
                        keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                        for (int i = 0; i < primaryKey.size(); i++) {
                            rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                        }

                        for (int i = primaryKey.size(); i < columns.size(); i++) {
                            String value = jedisCluster.hget(key, columns.get(i));
                            rowData.setField(i,BinaryStringData.fromString(value));
                        }

                        rows.add(rowData);



                    }else{
                        //Fuzzy matching ,gets the data of the entire table
                        String fuzzyKey = new StringBuffer(key).append("*").toString();
                        Set<String> keys = jedisCluster.keys(fuzzyKey);
                        for (String keyStr : keys) {
                            keySplit = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            rowData = new GenericRowData(columns.size());
                            for (int i = 0; i < primaryKey.size(); i++) {
                                rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                            }

                            for (int i = primaryKey.size(); i < columns.size(); i++) {
                                String value = jedisCluster.hget(keyStr, columns.get(i));
                                rowData.setField(i,BinaryStringData.fromString(value));
                            }

                            rows.add(rowData);


                        }
                    }

                    return rows;

                case RedisCommandOptions.HSCAN:
                    cursor = options.get(RedisOptions.CURSOR);
                    ScanResult<Map.Entry<String, String>> entries = jedisCluster.hscan(key, cursor);
                    List<Map.Entry<String, String>> result = entries.getResult();
                    keySplit = key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                    rowData = new GenericRowData(columns.size());
                    for (int i = 0; i < primaryKey.size(); i++) {
                        rowData.setField(i,BinaryStringData.fromString(keySplit[2 * primaryKey.size()]));
                    }

                    position = primaryKey.size();
                    for (int i = 0; i < result.size(); i++) {
                        value = result.get(i).getValue();
                        rowData.setField(position,BinaryStringData.fromString(value));
                        position++;
                    }
                    rows.add(rowData);
                    break;



                case RedisCommandOptions.LRANGE:
                    start = options.get(RedisOptions.START);
                    end = options.get(RedisOptions.END);
                    List<String> list = jedisCluster.lrange(key, start, end);
                    rowData = new GenericRowData(list.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    list.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.SMEMBERS:
                    Set<String> smembers = jedisCluster.smembers(key);
                    rowData = new GenericRowData(smembers.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    smembers.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;

                case RedisCommandOptions.ZRANGE:
                    start = options.get(RedisOptions.START);
                    end = options.get(RedisOptions.END);
                    Set<String> sets = jedisCluster.zrange(key, start, end);
                    rowData = new GenericRowData(sets.size() +1);
                    rowData.setField(0,BinaryStringData.fromString(key));
                    sets.forEach(s -> {
                        rowData.setField(position,BinaryStringData.fromString(s));
                        position++;});
                    rows.add(rowData);
                    break;


                default:
                    LOG.error("Cannot process such data type: {}", command);
                    break;
            }

            if(!command.toUpperCase().equals(RedisCommandOptions.HGETALL)){
                rows.add(rowData);


            }

        }else{
            LOG.error("Unsupport such {} mode",mode);

        }

        return rows;


    }

    @Override
    public void handleSplitsChanges(SplitsChange<RedisScanSourceSplit> splitsChanges) {
        LOG.debug("Handle split changes {}", splitsChanges);

        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        RedisScanSourceSplit sourceSplit = splitsChanges.splits().get(0);
        if (!(sourceSplit instanceof RedisScanSourceSplit)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SourceSplit type of %s is not supported.",
                            sourceSplit.getClass()));
        }

        this.currentSplit = (RedisScanSourceSplit) sourceSplit;
        this.finished = false;
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

        if(jedis != null){
            jedis.close();
        }

        if (jedisCluster != null){
            jedisCluster.close();
        }

    }
}
