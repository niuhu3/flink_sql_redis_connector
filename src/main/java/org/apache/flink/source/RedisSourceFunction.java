package org.apache.flink.source;


import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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

import java.util.List;
import java.util.Map;
import java.util.Set;


public class RedisSourceFunction extends RichSourceFunction<RowData>{

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceFunction.class);

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;
    private Jedis jedis;
    private JedisCluster jedisCluster;
    private String value;
    private String field;
    private String[] fields;
    private String cursor;
    private Long start;
    private Long end;
    private static int position = 1;
    private GenericRowData rowData;
    public RedisSourceFunction(ReadableConfig options, List<String> columns, List<String> primaryKey){
        this.options = Preconditions.checkNotNull(options);
        this.columns = Preconditions.checkNotNull(columns);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);

    }


    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {

        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password,"password is null,please set value for password");
        Integer expire = options.get(RedisOptions.EXPIRE);
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key,"key is null,please set value for key");
        String[] keyArr = key.split(RedisSplitSymbol.CLUSTER_NODES_SPLIT);
        String command = options.get(RedisOptions.COMMAND);
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
            JedisPool jedisPool = RedisUtil.getSingleJedisPool(mode, host, port, maxTotal,
                    maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);
            jedis = jedisPool.getResource();
            jedis.auth(password);



            switch (command.toUpperCase()){
                        case RedisCommandOptions.GET:
                            value = jedis.get(key);
                            rowData = new GenericRowData(2);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            rowData.setField(1,BinaryStringData.fromString(value));
                            break;

                        case RedisCommandOptions.HGET:
                            field= columns.get(0);
                            value = jedis.hget(key, field);
                            rowData = new GenericRowData(3);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            rowData.setField(1,BinaryStringData.fromString(field));
                            rowData.setField(2,BinaryStringData.fromString(value));
                            break;

                        case RedisCommandOptions.HGETALL:
                            if (keyArr.length > 1){
                                for (String str : keyArr) {
                                    rowData = new GenericRowData(columns.size());
                                    String[] strings = str.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    int j = 0;
                                    for (int i = 1; i < columns.size(); i++) {
                                        rowData.setField(j,BinaryStringData.fromString(strings[2*i]));
                                        j++;
                                    }

                                    for (int i = j; i < columns.size(); i++) {
                                        String value = jedis.hget(str, columns.get(i));
                                        rowData.setField(j,BinaryStringData.fromString(value));
                                        j++;
                                    }
                                    ctx.collect(rowData);
                                }

                            }else if(key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT).length == (primaryKey.size() * 2 + 1)){
                                rowData = new GenericRowData(columns.size());
                                rowData.setField(0,BinaryStringData.fromString(key));
                                for (int i = 1; i < columns.size(); i++) {
                                    String value = jedis.hget(key, columns.get(i));
                                    rowData.setField(position,BinaryStringData.fromString(value));
                                    position++;
                                }

                            }else{
                                //Fuzzy matching ,gets the data of the entire table
                                String fuzzyKey = new StringBuffer(key).append("*").toString();
                                Set<String> keys = jedis.keys(fuzzyKey);
                                for (String keyStr : keys) {
                                    position = 0;
                                    rowData = new GenericRowData(columns.size());
                                    if(primaryKey.size() == 1){
                                        String[] strings = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                        rowData.setField(0,BinaryStringData.fromString(strings[2]));
                                    }else if(primaryKey.size() > 1){
                                        String[] strings = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                        for (int i = 0; i < (strings.length / 2); i++) {
                                            rowData.setField(position,BinaryStringData.fromString(strings[(i + 1) * 2]));
                                            position++;
                                        }

                                    }


                                    for (int i = primaryKey.size(); i < columns.size(); i++) {
                                        String value = jedis.hget(keyStr, columns.get(i));
                                        rowData.setField(position,BinaryStringData.fromString(value));
                                        position++;
                                    }

                                    ctx.collect(rowData);

                                }
                            }

                            break;

                        case RedisCommandOptions.HSCAN:
                            cursor = options.get(RedisOptions.CURSOR);
                            ScanResult<Map.Entry<String, String>> entries = jedis.hscan(key, cursor);
                            List<Map.Entry<String, String>> result = entries.getResult();
                            rowData = new GenericRowData(result.size() * 2 +1);
                            rowData.setField(0,BinaryStringData.fromString(key));

                            for (int i = 0; i < result.size(); i++) {
                                field = result.get(i).getKey();
                                rowData.setField(position,BinaryStringData.fromString(field));
                                position++;
                                value = result.get(i).getValue();
                                rowData.setField(position,BinaryStringData.fromString(value));
                                position++;
                            }
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

                            break;

                        case RedisCommandOptions.SMEMBERS:
                            Set<String> smembers = jedis.smembers(key);
                            rowData = new GenericRowData(smembers.size() +1);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            smembers.forEach(s -> {
                                rowData.setField(position,BinaryStringData.fromString(s));
                                position++;});
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
                            break;


                        default:
                            LOG.error("Cannot process such data type: {}", command);
                            break;
                    }

                    if(!command.toUpperCase().equals(RedisCommandOptions.HGETALL)){
                        ctx.collect(rowData);
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

            if(keyArr.length > 0){{
                for (String key1 : keyArr) {
                    switch (command.toUpperCase()){
                        case RedisCommandOptions.GET:
                            value = jedisCluster.get(key);
                            rowData = new GenericRowData(2);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            rowData.setField(1,BinaryStringData.fromString(value));
                            break;

                        case RedisCommandOptions.HGET:
                            field = options.get(RedisOptions.FIELD);
                            value = jedisCluster.hget(key, field);
                            rowData = new GenericRowData(3);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            rowData.setField(1,BinaryStringData.fromString(field));
                            rowData.setField(2,BinaryStringData.fromString(value));
                            break;

                        case RedisCommandOptions.HGETALL:
                            if (keyArr.length > 1){
                                for (String str : keyArr) {
                                    rowData = new GenericRowData(columns.size());
                                    rowData.setField(0,BinaryStringData.fromString(str));
                                    for (int i = 1; i < columns.size(); i++) {
                                        String value = jedisCluster.hget(str, columns.get(i));
                                        rowData.setField(position,BinaryStringData.fromString(value));
                                        position++;
                                    }
                                    ctx.collect(rowData);
                                }

                            }else if(key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT).length == 3){
                                rowData = new GenericRowData(columns.size());
                                rowData.setField(0,BinaryStringData.fromString(key));
                                for (int i = 1; i < columns.size(); i++) {
                                    String value = jedisCluster.hget(key, columns.get(i));
                                    rowData.setField(position,BinaryStringData.fromString(value));
                                    position++;
                                }

                            }else if(key.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT).length <= 2){
                                //Fuzzy matching ,gets the data of the entire table
                                Set<String> keys = jedisCluster.keys(key);
                                for (String keyStr : keys) {
                                    position = 1;
                                    rowData = new GenericRowData(columns.size());
                                    if(primaryKey.size() == 1){
                                        String[] strings = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                        rowData.setField(0,BinaryStringData.fromString(strings[2]));
                                    }else if(primaryKey.size() > 1){
                                        String[] strings = keyStr.split(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                        rowData.setField(0,BinaryStringData.fromString(strings[2]));
                                    }


                                    for (int i = 1; i < columns.size(); i++) {
                                        String value = jedisCluster.hget(keyStr, columns.get(i));
                                        rowData.setField(position,BinaryStringData.fromString(value));
                                        position++;
                                    }

                                    ctx.collect(rowData);

                                }
                            }

                            break;

                        case RedisCommandOptions.HSCAN:
                            cursor = options.get(RedisOptions.CURSOR);
                            ScanResult<Map.Entry<String, String>> entries = jedisCluster.hscan(key, cursor);
                            List<Map.Entry<String, String>> result = entries.getResult();
                            rowData = new GenericRowData(result.size() * 2 +1);
                            rowData.setField(0,BinaryStringData.fromString(key));

                            for (int i = 0; i < result.size(); i++) {
                                field = result.get(i).getKey();
                                rowData.setField(position,BinaryStringData.fromString(field));
                                position++;
                                value = result.get(i).getValue();
                                rowData.setField(position,BinaryStringData.fromString(value));
                                position++;
                            }
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

                            break;

                        case RedisCommandOptions.SMEMBERS:
                            Set<String> smembers = jedisCluster.smembers(key);
                            rowData = new GenericRowData(smembers.size() +1);
                            rowData.setField(0,BinaryStringData.fromString(key));
                            smembers.forEach(s -> {
                                rowData.setField(position,BinaryStringData.fromString(s));
                                position++;});
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
                            break;


                        default:
                            LOG.error("Cannot process such data type: {}", command);
                            break;
                    }
                }
            }

                ctx.collect(rowData);
            }
        }else{
            LOG.error("Unsupport such {} mode",mode);
        }




    }

    @Override
    public void cancel() {

        if(jedis != null){
            jedis.close();
        }

        if(jedisCluster != null){
            jedisCluster.close();
        }

    }
}
