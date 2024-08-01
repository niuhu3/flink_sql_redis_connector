package org.apache.flink.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Set;

public class JedisClusterPipeline extends JedisCluster {
    public JedisClusterPipeline(Set<HostAndPort> jedisClusterNode, int connectionTimeout, int soTimeout, int maxAttempts, String password, final GenericObjectPoolConfig poolConfig) {
        super(jedisClusterNode,connectionTimeout, soTimeout, maxAttempts, password, poolConfig);
        super.connectionHandler = new JedisSlotAdvancedConnectionHandler(jedisClusterNode, poolConfig,
                connectionTimeout, soTimeout ,password);
    }

    public JedisSlotAdvancedConnectionHandler getConnectionHandler() {
        return (JedisSlotAdvancedConnectionHandler)this.connectionHandler;
    }

    /**
     * 刷新集群信息，当集群信息发生变更时调用
     * @param
     * @return
     */
    public void refreshCluster() {
        connectionHandler.renewSlotCache();
    }
}
