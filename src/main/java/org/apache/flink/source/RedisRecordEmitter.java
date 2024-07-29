package org.apache.flink.source;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.List;

public class RedisRecordEmitter<T> implements RecordEmitter<RowData,T,RedisSourceSplitState> {


    private List<String> primaryKey;
    private final SourceOutputWrapper<T> sourceOutputWrapper;

    public RedisRecordEmitter(List<String> primaryKey) {
        this.sourceOutputWrapper = new SourceOutputWrapper<>();
        this.primaryKey = primaryKey;
    }

    @Override
    public void emitRecord(RowData element, SourceOutput output, RedisSourceSplitState splitState) throws Exception {
        splitState.updateOffset(element);
        // Sink the record to source output.
        sourceOutputWrapper.setSourceOutput(output);
        output.collect(element);
    }


    private static class SourceOutputWrapper<T> implements Collector<T> {
        private SourceOutput<T> sourceOutput;

        @Override
        public void collect(T record) {
            sourceOutput.collect(record);
        }

        @Override
        public void close() {}

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }
    }
}
