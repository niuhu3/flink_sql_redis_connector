package org.apache.flink.source;

import org.apache.flink.common.RedisOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class RedisSourceSplitSerializer implements SimpleVersionedSerializer<RedisScanSourceSplit> {

    private ReadableConfig options;

    // This version should be bumped after modifying the MongoSourceSplit.
    public static final int CURRENT_VERSION = 0;

    public static final int SCAN_SPLIT_FLAG = 1;

    public RedisSourceSplitSerializer(ReadableConfig options){
        this.options = options;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RedisScanSourceSplit obj) throws IOException {
        // VERSION 0 serialization
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            serializeRedisSplit(out, obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    private void serializeRedisSplit(DataOutputStream out, RedisScanSourceSplit obj) throws IOException {

        if (obj instanceof RedisScanSourceSplit) {
            RedisScanSourceSplit split = (RedisScanSourceSplit) obj;
            out.writeInt(SCAN_SPLIT_FLAG);
            out.writeUTF(split.splitId());
            out.writeUTF(split.getKey());
            String field = options.get(RedisOptions.FIELD);

            if(field != null){
                out.writeUTF(split.getField());
            }



        }

    }

    @Override
    public RedisScanSourceSplit deserialize(int version, byte[] serialized) throws IOException {

        try (
                ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {
            int splitKind = in.readInt();
            if (splitKind == SCAN_SPLIT_FLAG) {
                return deserializeRedisScanSourceSplit(version, in);
            }
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }

    private RedisScanSourceSplit deserializeRedisScanSourceSplit(int version, DataInputStream in) throws IOException {
        switch (version) {
            case 0:
                String splitId = in.readUTF();
                String key = in.readUTF();
                String field = options.get(RedisOptions.FIELD);

                if(field != null){
                    return  new RedisScanSourceSplit(splitId, key, field);
                }

                return  new RedisScanSourceSplit(splitId, key, null);


            default:
                throw new IOException("Unknown version: " + version);

        }
    }
}
