package org.apache.flink.sink;

import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.JedisClusterPipeline;
import org.apache.flink.util.JedisSlotAdvancedConnectionHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.HashMap;
import java.util.List;


public class RedisSinkFunction extends RichSinkFunction<RowData>{

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);


    private ReadableConfig options;
    private List<String> primaryKey;
    private List<String> columns;
    private JedisPool jedisPool;
    private Jedis jedis;
    private Pipeline pipeline;
    private JedisClusterPipeline jedisClusterPipeline;
    private StringBuffer redisTableKey;
    private JedisSlotAdvancedConnectionHandler jedisSlotAdvancedConnectionHandler;
    private String value;

    public RedisSinkFunction(ReadableConfig options, List<String> columns, List<String> primaryKey){

        this.options = Preconditions.checkNotNull(options);
        this.columns = Preconditions.checkNotNull(columns);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);





    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize the redis client.
        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password, "password is null,please set value for password");
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key, "key is null,please set value for key");
        String command = options.get(RedisOptions.COMMAND);

        Preconditions.checkNotNull(command, "command is null,please set value for command");
        String mode = options.get(RedisOptions.MODE);
        Preconditions.checkNotNull(mode, "mode is null,please set value for mode");
        Integer maxIdle = options.get(RedisOptions.CONNECTION_MAX_IDLE);
        Integer maxTotal = options.get(RedisOptions.CONNECTION_MAX_TOTAL);
        Integer maxWaitMills = options.get(RedisOptions.CONNECTION_MAX_WAIT_MILLS);

        Boolean testOnBorrow = options.get(RedisOptions.CONNECTION_TEST_ON_BORROW);
        Boolean testOnReturn = options.get(RedisOptions.CONNECTION_TEST_ON_RETURN);
        Boolean testWhileIdle = options.get(RedisOptions.CONNECTION_TEST_WHILE_IDLE);

        if (mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())) {

            String host = options.get(RedisOptions.SINGLE_HOST);
            Integer port = options.get(RedisOptions.SINGLE_PORT);
            jedis = RedisUtil.getSingleJedis(mode, host, port, maxTotal,
                    maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);
            jedis.auth(password);
            pipeline = jedis.pipelined();

        } else if (mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())) {
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

            jedisClusterPipeline = RedisUtil.getJedisCluster(mode, host, password, port, maxTotal,
                    maxIdle, maxWaitMills, connTimeOut, soTimeOut, maxAttempts, testOnBorrow, testOnReturn, testWhileIdle);

            jedisSlotAdvancedConnectionHandler = jedisClusterPipeline.getConnectionHandler();

            //查询出 key 所在slot ,通过 slot 获取 JedisPool ,将key 按 JedisPool 分组
            jedisClusterPipeline.refreshCluster();
            int slot = JedisClusterCRC16.getSlot(key);
            jedisPool = jedisSlotAdvancedConnectionHandler.getJedisPoolFromSlot(slot);
            jedis = jedisPool.getResource();
            pipeline = jedis.pipelined();
        }
    }



    @Override
    public void invoke(RowData rowData, Context context) throws Exception {

        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password,"password is null,please set value for password");
        Integer expire = options.get(RedisOptions.EXPIRE);
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key,"key is null,please set value for key");
        String command = options.get(RedisOptions.COMMAND);
        Preconditions.checkNotNull(command,"command is null,please set value for command");
        String mode = options.get(RedisOptions.MODE);
        Preconditions.checkNotNull(command,"mode is null,please set value for mode");


        if (mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())) {

            switch (command.toUpperCase()){
                case RedisCommandOptions.SET:
                    value = rowData.getString(0).toString();
                    pipeline.set(String.valueOf(key),String.valueOf(value));
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
                    pipeline.hset(String.valueOf(redisTableKey),String.valueOf(field),String.valueOf(value));

                    if(expire != null){
                        pipeline.expire(String.valueOf(redisTableKey),expire);
                    }

                    break;

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
                    HashMap<String, String> data = new HashMap<>();
                    for (int i = 1; i < columns.size(); i++) {
                        if (!primaryKey.contains(columns.get(i))){
                            value = rowData.getString(i).toString();
                            data.put(columns.get(i),value);
                        }
                    }

                    pipeline.hmset(redisTableKey.toString(),data);

                    if(expire != null){
                        pipeline.expire(String.valueOf(redisTableKey),expire);
                    }

                    break;

                case RedisCommandOptions.LPUSH:

                    value = rowData.getString(0).toString();
                    pipeline.lpush(key,value);

                    break;

                case RedisCommandOptions.RPUSH:

                    value = rowData.getString(0).toString();
                    pipeline.rpush(key,value);

                    break;

                case RedisCommandOptions.SADD:
                    value = rowData.getString(0).toString();
                    pipeline.sadd(key,value);
                    break;

                default:
                    LOG.error("Cannot process such data type: {}", command);
                    break;
            }

            if(expire != null && (!command.toUpperCase().equals(RedisCommandOptions.HSET) && !command.toUpperCase().equals(RedisCommandOptions.HMSET)) ){
                pipeline.expire(String.valueOf(redisTableKey),expire);
            }

            pipeline.sync();



        } else if(mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())) {

            switch (command.toUpperCase()) {
                case RedisCommandOptions.SET:

                    value = rowData.getString(0).toString();
                    pipeline.set(key, value);

                    break;

                case RedisCommandOptions.HSET:

                    String field = columns.get(1);
                    //construct redis key:table_name:primary key col name: primary key value
                    redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);

                    for (int i = 0; i < primaryKey.size(); i++) {
                        if (primaryKey.size() <= 1) {
                            redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            redisTableKey.append(rowData.getString(i).toString());
                            break;
                        } else {
                            redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            redisTableKey.append(rowData.getString(i).toString());
                        }
                        redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                    }

                    value = rowData.getString(1).toString();
                    pipeline.hset(key, field, value);


                    if (expire != null) {
                        jedis.expire(String.valueOf(redisTableKey), expire);
                    }

                    break;

                case RedisCommandOptions.HMSET:

                    redisTableKey = new StringBuffer(key).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                    for (int i = 0; i < primaryKey.size(); i++) {
                        if (primaryKey.size() <= 1) {
                            redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            redisTableKey.append(rowData.getString(i).toString());
                            break;
                        } else {
                            redisTableKey.append(primaryKey.get(i)).append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                            redisTableKey.append(rowData.getString(i).toString());
                        }
                        redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);


                    }

                    HashMap<String, String> data = new HashMap<>();
                    for (int i = 1; i < columns.size(); i++) {
                        if (!primaryKey.contains(columns.get(i))) {
                            value = rowData.getString(i).toString();
                            data.put(columns.get(i), value);
                        }
                    }

                    pipeline.hmset(redisTableKey.toString(), data);
                    if (expire != null) {
                        pipeline.expire(String.valueOf(redisTableKey), expire);
                    }


                    break;

                case RedisCommandOptions.LPUSH:
                    value = rowData.getString(0).toString();
                    pipeline.lpush(key,value);

                    if(expire != null){
                        jedis.expire(String.valueOf(redisTableKey),expire);
                    }


                    break;


                case RedisCommandOptions.RPUSH:
                    value = rowData.getString(0).toString();
                    pipeline.rpush(key,value);


                    break;

                case RedisCommandOptions.SADD:
                    value = rowData.getString(0).toString();
                    pipeline.sadd(key,value);


                default:
                    LOG.error("Cannot process such data type: {}", command);
                    break;
            }

            if(expire != null && (!command.toUpperCase().equals(RedisCommandOptions.HSET) && !command.toUpperCase().equals(RedisCommandOptions.HMSET)) ){
                jedis.expire(String.valueOf(redisTableKey),expire);
            }

            pipeline.sync();


        }
        else{
            LOG.error("Unsupport such {} mode",mode);
        }

    }

    @Override
    public void close() throws Exception {
        if(jedis != null){
            jedis.close();
        }

        if(jedisClusterPipeline != null){
            jedisClusterPipeline.close();
        }

    }


}
