package org.apache.flink.util;


import org.apache.flink.common.RedisClusterMode;
import redis.clients.jedis.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class RedisUtil {


    public static  JedisPool jedisPool;
    public static  ShardedJedisPool shardedJedisPool;
    public static JedisCluster jedisCluster;

    public static JedisPool getSingleJedisPool(
            String mode, String host, int port,
            int maxTotal, int maxIdle , int maxWaitMills,
            boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle){

        if(jedisPool == null && mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())){
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            config.setMaxWaitMillis(maxWaitMills);
            config.setTestOnBorrow(testOnBorrow);
            config.setTestOnBorrow(testOnReturn);
            config.setTestOnBorrow(testWhileIdle);

            jedisPool = new JedisPool(config,host,port);

        }
        return jedisPool;

    }

    public static ShardedJedisPool getShardInfosJedisPool(String mode, String[] host, int[] port,
                                                          int maxTotal, int maxIdle , int maxWaitMills,
                                                          boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle){

        if(shardedJedisPool == null && mode.toUpperCase().equals(RedisClusterMode.SHARDING_CLUSTER.name())){

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(maxTotal);
            config.setMaxIdle(maxIdle);
            config.setMaxWaitMillis(maxWaitMills);
            config.setTestOnBorrow(testOnBorrow);
            config.setTestOnBorrow(testOnReturn);
            config.setTestOnBorrow(testWhileIdle);

            ArrayList<JedisShardInfo> shardInfos = new ArrayList<>();
            for (int i = 0; i < host.length; i++) {
                shardInfos.add(new JedisShardInfo(host[i],port[i]));
            }

            shardedJedisPool = new ShardedJedisPool(config,shardInfos);
        }
        return shardedJedisPool;
    }


        public static JedisCluster getJedisCluster(
                String mode, String[] host, String password, int[] port,
                int maxTotal, int maxIdle , int maxWaitMills ,
                int connTimeOut, int soTimeOut, int maxAttempts,
                boolean testOnBorrow, boolean testOnReturn, boolean testWhileIdle){

            if(jedisCluster == null && mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())){

                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxTotal(maxTotal);
                config.setMaxIdle(maxIdle);
                config.setMaxWaitMillis(maxWaitMills);
                config.setTestOnBorrow(false);
                config.setTestOnBorrow(testOnBorrow);
                config.setTestOnBorrow(testOnReturn);
                config.setTestOnBorrow(testWhileIdle);

                Set<HostAndPort> hostAndPorts = new HashSet<>();
                for (int i = 0; i < host.length; i++) {
                    hostAndPorts.add(new HostAndPort(host[i],port[i]));
                }

                jedisCluster = new JedisCluster(hostAndPorts,connTimeOut,soTimeOut,maxAttempts,password,config);

            }
            return jedisCluster;
        }




}
