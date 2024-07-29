package org.apache.flink.source;

import java.util.*;

public class RedisSourceEnumState {

    private final List<String> remainingKey;
    private final List<String> alreadProcessedyKey;
    private final List<RedisScanSourceSplit> remainingSplits;
    private final Map<String, RedisScanSourceSplit> assignedSplits;
    private final boolean initialized;

    public RedisSourceEnumState(List<String> remainingKey,
                                List<String> alreadProcessedyKey,
                                List<RedisScanSourceSplit> remainingSplits,
                                Map<String, RedisScanSourceSplit> assignedSplits,
                                boolean initialized){

        this.remainingKey = remainingKey;
        this.alreadProcessedyKey = alreadProcessedyKey;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.initialized = initialized;

    }

    public static RedisSourceEnumState initialState() {

      return new RedisSourceEnumState( new ArrayList<String>(), new ArrayList<>(), new ArrayList<>(), new HashMap<>(), false);
    }

    public List<String> getRemainingKey() {
        return remainingKey;
    }

    public List<String> getAlreadProcessedyKey() {
        return alreadProcessedyKey;
    }

    public Map<String, RedisScanSourceSplit> getAssignedScanSplits() {
        return assignedSplits;
    }

    public boolean isInitialized() {
        return  initialized;
    }

    public List<RedisScanSourceSplit> getRemainingSplits() {
        return remainingSplits;
    }
}
