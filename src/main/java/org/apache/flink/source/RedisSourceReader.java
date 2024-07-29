package org.apache.flink.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.function.Supplier;

public class RedisSourceReader<OUT> extends SingleThreadMultiplexSourceReaderBase<
        String, OUT, RedisScanSourceSplit, RedisSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSourceReader.class);

    public RedisSourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<String>> elementQueue,
                             Supplier<SplitReader<String, RedisScanSourceSplit>> splitReaderSupplier,
                             RecordEmitter<String, OUT, RedisSourceSplitState> recordEmitter,
                             RedisSourceReaderContext context) {

        super(elementQueue,splitReaderSupplier,recordEmitter,context.getConfiguration(),context);

    }


    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        return super.pollNext(output);
    }

    @Override
    protected void onSplitFinished(Map<String, RedisSourceSplitState> finishedSplitIds) {
        for (RedisSourceSplitState splitState : finishedSplitIds.values()) {
            RedisScanSourceSplit sourceSplit = splitState.toRedisSourceSplit();
            LOG.info("Split {} is finished.", sourceSplit.splitId());
        }
        context.sendSplitRequest();
    }

    @Override
    protected RedisSourceSplitState initializedState(RedisScanSourceSplit split) {
        if (split instanceof RedisScanSourceSplit) {
            return new RedisSourceSplitState(0,split);
        } else {
            throw new IllegalArgumentException("Unknown split type.");
        }    }

    @Override
    protected RedisScanSourceSplit toSplitType(String splitId, RedisSourceSplitState splitState) {
        return splitState.toRedisSourceSplit();
    }


}
