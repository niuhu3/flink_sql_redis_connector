package org.apache.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;

public class DefaultRedisSinkContext implements RedisSinkContext {

    private final Sink.InitContext initContext;
    private final RedisWriteOptions writeOptions;

    public DefaultRedisSinkContext(Sink.InitContext initContext,RedisWriteOptions writeOptions) {
        this.initContext = initContext;
        this.writeOptions = writeOptions;
    }



    @Override
    public Sink.InitContext getInitContext() {
        return initContext;
    }

    @Override
    public long processTime() {
        return initContext.getProcessingTimeService().getCurrentProcessingTime();
    }
}
