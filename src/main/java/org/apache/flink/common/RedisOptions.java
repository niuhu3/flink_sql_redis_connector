package org.apache.flink.common;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import redis.clients.jedis.Protocol;

import java.time.Duration;

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

    public static final ConfigOption<String> FIELD = ConfigOptions
            .key("field")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> CURSOR = ConfigOptions
            .key("cursor")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> START = ConfigOptions
            .key("start")
            .intType()
            .defaultValue(0);

    public static final ConfigOption<Integer> END = ConfigOptions
            .key("end")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Duration> LOOKUP_RETRY_INTERVAL =
            ConfigOptions.key("lookup.retry.interval")
                    .durationType()
                    .defaultValue(Duration.ofMillis(1000L))
                    .withDescription(
                            "Specifies the retry time interval if lookup records from database failed.");

    public static final ConfigOption<Integer> BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Specifies the maximum number of buffered rows per batch request.");

    public static final ConfigOption<Duration> BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Specifies the batch flush interval.");

    public static final ConfigOption<DeliveryGuarantee> DELIVERY_GUARANTEE =
            ConfigOptions.key("sink.delivery-guarantee")
                    .enumType(DeliveryGuarantee.class)
                    .defaultValue(DeliveryGuarantee.AT_LEAST_ONCE)
                    .withDescription(
                            "Optional delivery guarantee when committing. The exactly-once guarantee is not supported yet.");


    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

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
