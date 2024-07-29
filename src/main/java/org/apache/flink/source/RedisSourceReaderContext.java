package org.apache.flink.source;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.concurrent.atomic.AtomicInteger;

public class RedisSourceReaderContext implements SourceReaderContext{

    private final SourceReaderContext readerContext;
    private final AtomicInteger readCount = new AtomicInteger(0);


    public RedisSourceReaderContext(SourceReaderContext readerContext) {
        this.readerContext = readerContext;
    }

    @Override
    public SourceReaderMetricGroup metricGroup() {
        return readerContext.metricGroup();
    }

    @Override
    public Configuration getConfiguration() {
        return readerContext.getConfiguration();
    }

    @Override
    public String getLocalHostName() {
        return readerContext.getLocalHostName();
    }

    @Override
    public int getIndexOfSubtask() {
        return readerContext.getIndexOfSubtask();
    }

    @Override
    public void sendSplitRequest() {
        readerContext.sendSplitRequest();
    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {
        readerContext.sendSourceEventToCoordinator(sourceEvent);
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return readerContext.getUserCodeClassLoader();
    }

    public AtomicInteger getReadCount() {
        return readCount;
    }




}
