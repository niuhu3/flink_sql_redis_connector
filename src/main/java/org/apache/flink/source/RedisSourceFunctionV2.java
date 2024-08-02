package org.apache.flink.source;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;


public class RedisSourceFunctionV2<OUT> implements
        Source<OUT, RedisScanSourceSplit,RedisSourceEnumState>, ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceFunctionV2.class);

    private ReadableConfig options;
    private List<String> primaryKey;
    private  List<String> columns;


    /** The boundedness for redis source. */
    private final Boundedness boundedness;


    public RedisSourceFunctionV2(ReadableConfig options, Boundedness boundedness, List<String> primaryKey, List<String> columns){
        this.options = Preconditions.checkNotNull(options);
        this.boundedness = Preconditions.checkNotNull(boundedness);
        this.primaryKey = primaryKey;
        this.columns = columns;

    }


    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<RedisScanSourceSplit, RedisSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RedisScanSourceSplit> enumContext,
            RedisSourceEnumState checkpoint) throws Exception {
        RedisSplitAssigner splitAssigner = new RedisSplitAssigner(options , checkpoint);
        return new RedisEnumerator(boundedness, enumContext, splitAssigner);
    }


    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
        //创建一个新的enumerator
        RedisSourceEnumState initialState = RedisSourceEnumState.initialState();
        RedisSplitAssigner splitAssigner =
                new RedisSplitAssigner(options , initialState);
        return new RedisEnumerator(boundedness, enumContext, splitAssigner);

    }


    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return new  RedisSourceSplitSerializer(options);
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return new  RedisSourceSplitSerializer(options);
    }

    @Override
    public SourceReader<OUT,RedisScanSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {

        FutureCompletingBlockingQueue<RecordsWithSplitIds<String>> elementsQueue =
                new FutureCompletingBlockingQueue<>();

        RedisSourceReaderContext redisReaderContext = new RedisSourceReaderContext(readerContext);

        Supplier<SplitReader<RowData, RedisScanSourceSplit>> splitReaderSupplier =
                () ->
                        new RedisSourceSplitReader(options,redisReaderContext,primaryKey,columns);


        return new RedisSourceReader(elementsQueue,splitReaderSupplier,new RedisRecordEmitter(primaryKey),redisReaderContext);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return null;
    }
}
