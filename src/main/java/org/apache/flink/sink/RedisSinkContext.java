package org.apache.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;

public interface RedisSinkContext {

    /** Returns the current sink's init context. */
    Sink.InitContext getInitContext();

    /** Returns the current process time in flink. */
    long processTime();

}
