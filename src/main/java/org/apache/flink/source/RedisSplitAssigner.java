package org.apache.flink.source;

import org.apache.flink.common.RedisClusterMode;
import org.apache.flink.common.RedisCommandOptions;
import org.apache.flink.common.RedisOptions;
import org.apache.flink.common.RedisSplitSymbol;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;


public class RedisSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSplitAssigner.class);

    private ReadableConfig options;
    private RedisSourceEnumState initialState;
    private boolean initialized;
    private final LinkedList<String> remainingKey;
    private final List<String> alreadProcessedyKey;
    private final LinkedList<RedisScanSourceSplit> remainingSplits;
    private final Map<String, RedisScanSourceSplit> assignedSplits;


    public RedisSplitAssigner(ReadableConfig options, RedisSourceEnumState initialState) {
        this.options = options;
        this.initialState = initialState;
        this.remainingKey = new LinkedList<>(initialState.getRemainingKey());
        this.alreadProcessedyKey = initialState.getAlreadProcessedyKey();
        this.assignedSplits = initialState.getAssignedScanSplits();
        this.remainingSplits = new LinkedList<>(initialState.getRemainingSplits());
        this.initialized = initialState.isInitialized();
    }


    public void open() {

        LOG.info("Redis split assigner is opening.");

        String key = options.get(RedisOptions.KEY);

        if (!initialized) {

            remainingKey.add(key);
        }
            initialized = true;

    }








    public void addSplitsBack(Collection<RedisScanSourceSplit> splits) {
        for (RedisScanSourceSplit split : splits) {
            if (split instanceof RedisScanSourceSplit) {
                remainingSplits.add((RedisScanSourceSplit) split);
                // we should remove the add-backed splits from the assigned list,
                // because they are failed
                assignedSplits.remove(split.splitId());
            }
        }
    }


    public boolean noMoreSplits() {
        checkState(initialized, "The noMoreSplits method was called but not initialized.");
        return remainingKey.isEmpty() && remainingSplits.isEmpty();

    }

    public Optional<RedisScanSourceSplit> getNext() {

        if (!remainingSplits.isEmpty()) {
            // return remaining splits firstly
            RedisScanSourceSplit split = remainingSplits.poll();
            assignedSplits.put(split.splitId(), split);
            return Optional.of(split);
        } else {
            // it's turn for next collection
            String nextKey = remainingKey.poll();
            if (nextKey != null) {
                // split the given collection into chunks (scan splits)

                String field = options.get(RedisOptions.FIELD);
                RedisScanSourceSplit split = new RedisScanSourceSplit(nextKey, nextKey,field);
                remainingSplits.add(split);
                alreadProcessedyKey.add(nextKey);
                return getNext();
            } else {
                return Optional.empty();
            }
        }

    }


    public void close() {

    }

    public RedisSourceEnumState snapshotState(long checkpointId) {

        return new RedisSourceEnumState(remainingKey,alreadProcessedyKey,remainingSplits,assignedSplits,initialized);
    }
}
