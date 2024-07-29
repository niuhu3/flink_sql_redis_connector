package org.apache.flink.source;

import org.apache.flink.table.data.RowData;

public class RedisSourceSplitState {

    private int offset;

    private RedisScanSourceSplit sourceSplit;


    public RedisSourceSplitState(
            int offset, RedisScanSourceSplit sourceSplit){
        this.offset = offset;
        this.sourceSplit = sourceSplit;

    }

    public RedisScanSourceSplit toRedisSourceSplit(){
        return new RedisScanSourceSplit(sourceSplit.splitId(), sourceSplit.getKey());
    }

    public void updateOffset(RowData element) {
        offset++;
    }
}

