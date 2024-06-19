package org.apache.flink.common;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import redis.clients.jedis.Protocol;

public class RedisOptions {

    private RedisOptions(){}

    public static final ConfigOption<String> MODE = ConfigOptions
            .key("mode")
            .stringType()
            .defaultValue("single");
    public static final ConfigOption<String> SINGLE_HOST = ConfigOptions
            .key("single.host")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> SINGLE_PORT = ConfigOptions
            .key("single.port")
            .intType()
            .defaultValue(Protocol.DEFAULT_PORT);

    public static final ConfigOption<String> CLUSTER_NODES = ConfigOptions
            .key("cluster.nodes")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> KEY = ConfigOptions
            .key("key")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> EXPIRE = ConfigOptions
            .key("expire")
            .intType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> COMMAND = ConfigOptions
            .key("command")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PRIMARY_KEY = ConfigOptions
            .key("primary.key")
            .stringType()
            .defaultValue("id");

    public static final ConfigOption<String> FIELD = ConfigOptions
            .key("field")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> CURSOR = ConfigOptions
            .key("cursor")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Long> START = ConfigOptions
            .key("start")
            .longType()
            .noDefaultValue();

    public static final ConfigOption<Long> END = ConfigOptions
            .key("end")
            .longType()
            .noDefaultValue();

    public static final ConfigOption<Integer> CONNECTION_MAX_WAIT_MILLS = ConfigOptions
            .key("connection.max.wait-mills")
            .intType()
            .defaultValue(-1);

    public static final ConfigOption<Integer> TTL_SEC = ConfigOptions
            .key("ttl-sec")
            .intType()
            .noDefaultValue();
    public static final ConfigOption<Integer> CONNECTION_TIMEOUT_MS = ConfigOptions
            .key("connection.timeout-ms")
            .intType()
            .defaultValue(Protocol.DEFAULT_TIMEOUT);

    public static final ConfigOption<Integer> SO_TIMEOUT_MS = ConfigOptions
            .key("so.timeout-ms")
            .intType()
            .defaultValue(Protocol.DEFAULT_TIMEOUT);

    public static final ConfigOption<Integer> MAX_ATTEMPTS = ConfigOptions
            .key("max.attempts")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Integer> CONNECTION_MAX_TOTAL = ConfigOptions
            .key("connection.max-total")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);

    public static final ConfigOption<Integer> CONNECTION_MAX_IDLE = ConfigOptions
            .key("connection.max-idle")
            .intType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_MAX_IDLE);
    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_BORROW = ConfigOptions
            .key("connection.test-on-borrow")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW);

    public static final ConfigOption<Boolean> CONNECTION_TEST_ON_RETURN = ConfigOptions
            .key("connection.test-on-return")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN);

    public static final ConfigOption<Boolean> CONNECTION_TEST_WHILE_IDLE = ConfigOptions
            .key("connection.test-while-idle")
            .booleanType()
            .defaultValue(GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);

    public static final ConfigOption<String> LOOKUP_ADDITIONAL_KEY = ConfigOptions
            .key("lookup.additional-key")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions
            .key("lookup.cache.max-rows")
            .intType()
            .defaultValue(-1);
    public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC = ConfigOptions
            .key("lookup.cache.ttl-sec")
            .intType()
            .defaultValue(-1);
}
