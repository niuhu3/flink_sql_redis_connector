package org.apache.flink.source;

import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;


public class RedisSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSplitAssigner.class);

    private ReadableConfig options;
    private RedisSourceEnumState initialState;
    private boolean initialized;
    private Jedis jedis;
    private JedisCluster jedisCluster;
    private final LinkedList<String> remainingKey;
    private final List<String> alreadProcessedyKey;
    private final LinkedList<RedisScanSourceSplit> remainingSplits;
    private final Map<String, RedisScanSourceSplit> assignedSplits;


    public RedisSplitAssigner(ReadableConfig options, RedisSourceEnumState initialState) {
        this.options = options;
        this.initialState = initialState;
        this.remainingKey = new LinkedList<>(initialState.getRemainingKey());
        this.alreadProcessedyKey = initialState.getAlreadProcessedyKey();
        this.assignedSplits = initialState.getAssignedScanSplits();
        this.remainingSplits = new LinkedList<>(initialState.getRemainingSplits());
        this.initialized = initialState.isInitialized();
    }


    public void open() {

        LOG.info("Redis split assigner is opening.");

        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password,"password is null,please set value for password");
        Integer expire = options.get(RedisOptions.EXPIRE);
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key,"key is null,please set value for key");
        String[] keyArr = key.split(RedisSplitSymbol.CLUSTER_NODES_SPLIT);
        String command = options.get(RedisOptions.COMMAND);

        // judge if command is redis set data command and stop method
        List<String> sourceCommand = Arrays.asList(RedisCommandOptions.SET, RedisCommandOptions.HSET, RedisCommandOptions.HMSET, RedisCommandOptions.LPUSH,
                RedisCommandOptions.RPUSH, RedisCommandOptions.SADD);
        if(sourceCommand.contains(command.toUpperCase())){ return;}

        Preconditions.checkNotNull(command,"command is null,please set value for command");
        String mode = options.get(RedisOptions.MODE);
        Preconditions.checkNotNull(command,"mode is null,please set value for mode");
        Integer maxIdle = options.get(RedisOptions.CONNECTION_MAX_IDLE);
        Integer maxTotal = options.get(RedisOptions.CONNECTION_MAX_TOTAL);
        Integer maxWaitMills = options.get(RedisOptions.CONNECTION_MAX_WAIT_MILLS);

        Boolean testOnBorrow = options.get(RedisOptions.CONNECTION_TEST_ON_BORROW);
        Boolean testOnReturn = options.get(RedisOptions.CONNECTION_TEST_ON_RETURN);
        Boolean testWhileIdle = options.get(RedisOptions.CONNECTION_TEST_WHILE_IDLE);

        if (!initialized) {

            remainingKey.add(key);
            if(mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())) {

                String host = options.get(RedisOptions.SINGLE_HOST);
                Integer port = options.get(RedisOptions.SINGLE_PORT);
                jedis = RedisUtil.getSingleJedis(mode, host, port, maxTotal,
                        maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);

                jedis.auth(password);
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


        }
            initialized = true;

    }





    }


    public void addSplitsBack(Collection<RedisScanSourceSplit> splits) {
        for (RedisScanSourceSplit split : splits) {
            if (split instanceof RedisScanSourceSplit) {
                remainingSplits.add((RedisScanSourceSplit) split);
                // we should remove the add-backed splits from the assigned list,
                // because they are failed
                assignedSplits.remove(split.splitId());
            }
        }
    }


    public boolean noMoreSplits() {
        checkState(initialized, "The noMoreSplits method was called but not initialized.");
        return remainingKey.isEmpty() && remainingSplits.isEmpty();

    }

    public Optional<RedisScanSourceSplit> getNext() {

        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            RedisScanSourceSplit split = remainingSplits.poll();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextKey = remainingKey.poll();
            if (nextKey != null) {
                // split the given collection into chunks (scan splits)

                String field = options.get(RedisOptions.FIELD);
                RedisScanSourceSplit split = new RedisScanSourceSplit(nextKey, nextKey,field);
                remainingSplits.add(split);
                alreadProcessedyKey.add(nextKey);
                return getNext();
            } else {
                return Optional.empty();
            }
        }

    }

    private List<RedisScanSourceSplit> searchData(String nextKey) {
        return  null;
    }

    public void close() {

        if(jedis != null){
            jedis.close();
        }

        if(jedisCluster != null){
            jedisCluster.close();
        }

    }

    public RedisSourceEnumState snapshotState(long checkpointId) {

        return new RedisSourceEnumState(remainingKey,alreadProcessedyKey,remainingSplits,assignedSplits,initialized);
    }
}
