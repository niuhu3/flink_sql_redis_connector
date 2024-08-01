package org.apache.flink.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

import java.util.Set;

public class JedisSlotAdvancedConnectionHandler extends JedisSlotBasedConnectionHandler {

    public JedisSlotAdvancedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout,String password) {
        super(nodes, poolConfig, connectionTimeout, soTimeout, password);
    }

    public JedisPool getJedisPoolFromSlot(int slot) {
        JedisPool connectionPool = cache.getSlotPool(slot);
        if (connectionPool != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return connectionPool;
        } else {
            renewSlotCache(); //It's abnormal situation for cluster mode, that we have just nothing for slot, try to rediscover state
            connectionPool = cache.getSlotPool(slot);
            if (connectionPool != null) {
                return connectionPool;
            } else {
                throw new JedisNoReachableClusterNodeException("No reachable node in cluster for slot " + slot);
            }
        }
    }
}
