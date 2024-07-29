package org.apache.flink.sink;


import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RedisWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.class);

    private ReadableConfig options;
    private final RedisWriteOptions writeOptions;
    private List<String> primaryKey;
    private List<String> columns;
    private final RedisSinkContext sinkContext;
    private final MailboxExecutor mailboxExecutor;
    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0L;
    private volatile long ackTime = Long.MAX_VALUE;
    private final List<RowData> bulkRequests = new ArrayList<>();
    private final Collector<RowData> collector;
    private final Counter numRecordsOut;
    private Jedis jedis;
    private Pipeline pipeline;
    private JedisCluster jedisCluster;
    private StringBuffer redisTableKey;
    private String value;
    private final boolean flushOnCheckpoint;

    private long diff;

    private final long batchIntervalMs;
    private final int batchSize;
    private transient volatile boolean closed = false;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;

    public RedisWriter(
            ReadableConfig options,
            List<String> primaryKey,
            List<String> columns,
            Sink.InitContext context,
            RedisWriteOptions writeOptions) {

        this.options = Preconditions.checkNotNull(options);
        this.primaryKey = Preconditions.checkNotNull(primaryKey);
        this.columns = Preconditions.checkNotNull(columns);
        this.writeOptions = checkNotNull(writeOptions);
        this.batchIntervalMs = writeOptions.getBatchIntervalMs();
        this.batchSize = writeOptions.getBatchSize();
        this.flushOnCheckpoint = writeOptions.flushOnCheckpoint();

        checkNotNull(context);
        this.mailboxExecutor = checkNotNull(context.getMailboxExecutor());

        SinkWriterMetricGroup metricGroup = checkNotNull(context.metricGroup());
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);

        this.numRecordsOut = metricGroup.getNumRecordsSendCounter();
        this.collector = new ListCollector<>(this.bulkRequests);


        this.sinkContext = new DefaultRedisSinkContext(context,writeOptions);

        // Initialize the redis client.
        String password = options.get(RedisOptions.PASSWORD);
        Preconditions.checkNotNull(password,"password is null,please set value for password");
        String key = options.get(RedisOptions.KEY);
        Preconditions.checkNotNull(key,"key is null,please set value for key");
        String command = options.get(RedisOptions.COMMAND);

        Preconditions.checkNotNull(command,"command is null,please set value for command");
        String mode = options.get(RedisOptions.MODE);
        Preconditions.checkNotNull(mode,"mode is null,please set value for mode");
        Integer maxIdle = options.get(RedisOptions.CONNECTION_MAX_IDLE);
        Integer maxTotal = options.get(RedisOptions.CONNECTION_MAX_TOTAL);
        Integer maxWaitMills = options.get(RedisOptions.CONNECTION_MAX_WAIT_MILLS);

        Boolean testOnBorrow = options.get(RedisOptions.CONNECTION_TEST_ON_BORROW);
        Boolean testOnReturn = options.get(RedisOptions.CONNECTION_TEST_ON_RETURN);
        Boolean testWhileIdle = options.get(RedisOptions.CONNECTION_TEST_WHILE_IDLE);


        if(mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())){

            String host = options.get(RedisOptions.SINGLE_HOST);
            Integer port = options.get(RedisOptions.SINGLE_PORT);
            jedis = RedisUtil.getSingleJedis(mode, host, port, maxTotal,
                    maxIdle, maxWaitMills, testOnBorrow, testOnReturn, testWhileIdle);
            jedis.auth(password);
            pipeline = jedis.pipelined();

        }else if(mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())) {
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



        boolean flushOnlyOnCheckpoint = batchIntervalMs == -1 && batchSize == -1;

        if (!flushOnlyOnCheckpoint && batchIntervalMs > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("redis-writer"));

            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (RedisWriter.this) {
                                    if (!closed && isOverMaxBatchIntervalLimit()) {
                                        try {
                                            doBulkWrite();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            batchIntervalMs,
                            batchIntervalMs,
                            TimeUnit.MILLISECONDS);
        }
    }


    void doBulkWrite() throws IOException {
        if (bulkRequests.isEmpty()) {
            // no records to write
            return;
        }

        int maxRetries = writeOptions.getMaxRetries();
        long retryIntervalMs = writeOptions.getRetryIntervalMs();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                lastSendTime = System.currentTimeMillis();
                ackTime = System.currentTimeMillis();
                String mode = options.get(RedisOptions.MODE);
                long start = System.currentTimeMillis();
                if(mode.toUpperCase().equals(RedisClusterMode.SINGLE.name())){
                    insertData(bulkRequests);

                }
                long end = System.currentTimeMillis();
                diff += end-start;
                System.out.println("total:"+(diff));

                bulkRequests.clear();
                break;
            } catch (Exception e) {
                LOG.debug("Bulk Write to Redis failed, retry times = {}", i, e);
                if (i >= maxRetries) {
                    LOG.error("Bulk Write to Redis failed", e);
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(retryIntervalMs * (i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException(
                            "Unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }

    @Override
    public synchronized void write(IN element, Context context)
            throws IOException, InterruptedException {
        checkFlushException();

        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        RowData rowData = (RowData) element;
        numRecordsOut.inc();
        collector.collect(rowData);
        //System.out.println(isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit());
        if (isOverMaxBatchSizeLimit() || isOverMaxBatchIntervalLimit()) {
            doBulkWrite();
        }
    }

    @Override
    public synchronized void flush(boolean endOfInput) throws IOException {
        checkFlushException();

        checkpointInProgress = true;
        while (!bulkRequests.isEmpty() && (flushOnCheckpoint || endOfInput)) {
            doBulkWrite();
        }
        checkpointInProgress = false;
    }

    @Override
    public synchronized void close() throws Exception {
        if (!closed) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }

            if (!bulkRequests.isEmpty()) {
                try {
                    doBulkWrite();
                } catch (Exception e) {
                    LOG.error("Writing data to redis failed when closing RedisWriter", e);
                    throw new IOException("Writing records to Redis failed.", e);
                } finally {
                    if(jedis != null){
                        jedis.close();
                    }else if(jedisCluster != null){
                        jedisCluster.close();
                    }

                    closed = true;
                }
            } else {
                if(jedis != null){
                    jedis.close();
                }else if(jedisCluster != null){
                    jedisCluster.close();
                }
                closed = true;
            }
        }
    }



    private boolean isOverMaxBatchSizeLimit() {
        return batchSize != -1 && bulkRequests.size() >= batchSize;
    }

    private boolean isOverMaxBatchIntervalLimit() {
        long lastSentInterval = System.currentTimeMillis() - lastSendTime;
        return batchIntervalMs != -1 && lastSentInterval >= batchIntervalMs;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Redis failed.", flushException);
        }
    }


    public void insertData(List<RowData> rows){

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
                    for (RowData rowData : rows){
                        value = rowData.getString(0).toString();
                        pipeline.set(String.valueOf(key),String.valueOf(value));
                    }
                    break;

                case RedisCommandOptions.HSET:

                    for (RowData rowData : rows){
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
                    }

                    break;

                case RedisCommandOptions.HMSET:


                    for(RowData rowData : rows){
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
                                pipeline.hset(String.valueOf(redisTableKey),String.valueOf(columns.get(i)),String.valueOf(value));
                            }
                        }

                        if(expire != null){
                            pipeline.expire(String.valueOf(redisTableKey),expire);
                        }
                    }

                    break;

                case RedisCommandOptions.LPUSH:

                    for (RowData rowData : rows){
                        value = rowData.getString(0).toString();
                        pipeline.lpush(key,value);
                    }

                    break;

                case RedisCommandOptions.RPUSH:

                    for (RowData rowData : rows){
                        value = rowData.getString(0).toString();
                        pipeline.rpush(key,value);
                    }

                    break;

                case RedisCommandOptions.SADD:
                    for (RowData rowData : rows){
                        value = rowData.getString(0).toString();
                        pipeline.sadd(key,value);
                    }
                    break;

                default:
                    LOG.error("Cannot process such data type: {}", command);
                    break;
            }

            if(expire != null && (!command.toUpperCase().equals(RedisCommandOptions.HSET) && !command.toUpperCase().equals(RedisCommandOptions.HMSET)) ){
                pipeline.expire(String.valueOf(redisTableKey),expire);
            }

            pipeline.sync();



        }
       /* else if(mode.toUpperCase().equals(RedisClusterMode.CLUSTER.name())){
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

                    if(expire != null){
                        jedis.expire(String.valueOf(redisTableKey),expire);
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
                        redisTableKey.append(RedisSplitSymbol.CLUSTER_HOST_PORT_SPLIT);
                    }

                    for (int i = 1; i < columns.size(); i++) {
                        value = rowData.getString(i).toString();
                        jedisCluster.hset(String.valueOf(redisTableKey),String.valueOf(columns.get(i)),String.valueOf(value));
                    }

                    if(expire != null){
                        jedis.expire(String.valueOf(redisTableKey),expire);
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

            if(expire != null && (!command.toUpperCase().equals(RedisCommandOptions.HSET) && !command.toUpperCase().equals(RedisCommandOptions.HMSET)) ){
                jedis.expire(String.valueOf(redisTableKey),expire);
            }


        }*/
        else{
            LOG.error("Unsupport such {} mode",mode);
        }

    }
}
