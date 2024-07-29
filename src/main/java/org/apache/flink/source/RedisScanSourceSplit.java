package org.apache.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

public class RedisScanSourceSplit extends RedisSourceSplit {

    private static final long serialVersionUID = 1L;

    private final String key;

    private  String field;


    public RedisScanSourceSplit(String splitId, String key){

        this(splitId,key,null);
    }

    public RedisScanSourceSplit(String splitId, String key, String field){
        super(splitId);
        this.key = key;
        this.field = field;
    }




    public String getKey() {
        return key;
    }

    public String getField() {
        return field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        RedisScanSourceSplit that = (RedisScanSourceSplit) o;
        return Objects.equals(splitId, that.splitId) &&
                Objects.equals(key, that.key) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, key, field);
    }
}
