package org.apache.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

public class RedisEnumerator implements SplitEnumerator<RedisScanSourceSplit,RedisSourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisEnumerator.class);

    private final Boundedness boundedness;
    private final SplitEnumeratorContext<RedisScanSourceSplit> context;
    private final RedisSplitAssigner splitAssigner;
    private final TreeSet<Integer> readersAwaitingSplit;

    public RedisEnumerator(Boundedness boundedness,
                           SplitEnumeratorContext context,
                           RedisSplitAssigner splitAssigner){
        this.boundedness = boundedness;
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }


    @Override
    public void start() {
        splitAssigner.open();
    }

    @Override
    public void addSplitsBack(List<RedisScanSourceSplit> splits, int subtaskId) {
        LOG.debug("Redis Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to RedisEnumerator.", subtaskId);
    }


    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();

    }

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            // close idle readers
            if (splitAssigner.noMoreSplits() && boundedness == Boundedness.BOUNDED) {
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                LOG.info(
                        "All scan splits have been assigned, closing idle reader {}", nextAwaiting);
                continue;
            }

            Optional<RedisScanSourceSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final RedisScanSourceSplit redisSplit = split.get();
                context.assignSplit(redisSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", redisSplit, nextAwaiting);
                break;
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }




    }

    @Override
    public RedisSourceEnumState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        splitAssigner.close();
    }
}
