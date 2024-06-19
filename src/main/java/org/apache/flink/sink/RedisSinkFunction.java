package org.apache.flink.sink;

import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.util.List;


public class RedisSinkFunction extends RichSinkFunction<RowData>{

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;
    private String fields;
    private Jedis jedis;
    private JedisCluster jedisCluster;
    private String[] fieldsArr;
    private StringBuffer redisTableKey;
    private String value;

    public RedisSinkFunction(ReadableConfig options, List<String> columns, List<String> primaryKey){

        this.options = Preconditions.checkNotNull(options);
        this.columns = Preconditions.checkNotNull(columns);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);
    }


    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        String password = options.get(RedisOptions.PASSWORD);
        Integer expire = options.get(RedisOptions.EXPIRE);
        String keyStr = options.get(RedisOptions.KEY);
        String[] keyArr = keyStr.split(RedisSplitSymbol.CLUSTER_NODES_SPLIT);
        String command = options.get(RedisOptions.COMMAND);
        String mode = options.get(RedisOptions.MODE);
        Integer maxIdle = options.get(RedisOptions.CONNECTION_MAX_IDLE);
        Integer maxTotal = options.get(RedisOptions.CONNECTION_MAX_TOTAL);
        Integer maxWaitMills = options.get(RedisOptions.CONNECTION_MAX_WAIT_MILLS);

        Boolean testOnBorrow = options.get(RedisOptions.CONNECTION_TEST_ON_BORROW);
        Boolean testOnReturn = options.get(RedisOptions.CONNECTION_TEST_ON_RETURN);
        Boolean testWhileIdle = options.get(RedisOptions.CONNECTION_TEST_WHILE_IDLE);


        if (mode != null && mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())) {

            String host = options.get(RedisOptions.SINGLE_HOST);
            Integer port = options.get(RedisOptions.SINGLE_PORT);
            JedisPool jedisPool = RedisUtil.getSingleJedisPool(mode, host, port, maxTotal,
                    maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);
            jedis = jedisPool.getResource();
            jedis.auth(password);


            if(command != null){
                for (String key : keyArr) {
                    switch (command.toUpperCase()){
                        case RedisCommandOptions.SET:
                            value = rowData.getString(0).toString();
                            jedis.set(String.valueOf(key),String.valueOf(value));
                            break;

                        case RedisCommandOptions.HSET:

                            String field = columns.get(1);
                            //construct redis key:table_name:primary key col name: primary key value
                            redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                if(primaryKey.size() <= 1){
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                    break;
                                }else{
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                }
                                redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            }

                            value = rowData.getString(1).toString();
                            jedis.hset(String.valueOf(redisTableKey),String.valueOf(field),String.valueOf(value));

                        case RedisCommandOptions.HMSET:
                            //construct redis key:table_name:primary key col name: primary key value
                            redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                if(primaryKey.size() <= 1){
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                    break;
                                }else{
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                }
                                if (i != primaryKey.size() -1){
                                    redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                }

                            }
                            for (int i = 1; i < columns.size(); i++) {
                                if (!primaryKey.contains(columns.get(i))){
                                    value = rowData.getString(i).toString();
                                    jedis.hset(String.valueOf(redisTableKey),String.valueOf(columns.get(i)),String.valueOf(value));
                                }
                            }

                            break;

                        case RedisCommandOptions.LPUSH:
                            value = rowData.getString(0).toString();
                            jedis.lpush(key,value);

                            break;


                        case RedisCommandOptions.RPUSH:
                            value = rowData.getString(0).toString();
                            jedis.rpush(key,value);

                            break;

                        case RedisCommandOptions.SADD:
                            value = rowData.getString(0).toString();
                            jedis.sadd(key,value);
                            break;

                        default:
                            LOG.error("Cannot process such data type: {}", command);
                            break;
                    }
                }




            }

        }else if(mode != null && mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())){
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

            if(command != null){
                for (String key : keyArr) {
                    switch (command.toUpperCase()){
                        case RedisCommandOptions.SET:
                            value = rowData.getString(0).toString();
                            jedisCluster.set(String.valueOf(key),String.valueOf(value));
                            break;

                        case RedisCommandOptions.HSET:

                            String field = columns.get(1);
                            //construct redis key:table_name:primary key col name: primary key value
                            redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                if(primaryKey.size() <= 1){
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                    break;
                                }else{
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                }
                                redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            }

                            value = rowData.getString(1).toString();
                            jedisCluster.hset(String.valueOf(redisTableKey),String.valueOf(field),String.valueOf(value));

                        case RedisCommandOptions.HMSET:
                            //construct redis key:table_name:primary key col name: primary key value
                            redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                            for (int i = 0; i < primaryKey.size(); i++) {
                                if(primaryKey.size() <= 1){
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                    break;
                                }else{
                                    redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                                    redisTableKey.append(rowData.getString(i).toString());
                                }
                                redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            }

                            for (int i = 1; i < columns.size(); i++) {
                                value = rowData.getString(i).toString();
                                jedisCluster.hset(String.valueOf(redisTableKey),String.valueOf(columns.get(i)),String.valueOf(value));
                            }

                            break;

                        case RedisCommandOptions.LPUSH:
                            value = rowData.getString(0).toString();
                            jedisCluster.lpush(key,value);

                            break;


                        case RedisCommandOptions.RPUSH:
                            value = rowData.getString(0).toString();
                            jedisCluster.rpush(key,value);

                            break;

                        case RedisCommandOptions.SADD:
                            value = rowData.getString(0).toString();
                            jedisCluster.sadd(key,value);
                            break;


                        default:
                            LOG.error("Cannot process such data type: {}", command);
                            break;
                    }
                }




            }


        }else{
            LOG.error("Unsupport such {} mode",mode);
        }

    }

    @Override
    public void close() throws Exception {
        if(jedis != null){
            jedis.close();
        }

        if(jedisCluster != null){
            jedisCluster.close();
        }

    }
}
