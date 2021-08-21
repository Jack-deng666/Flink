package com.jack.sourceData;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 16:40
 */
public class SourceData {
    public StreamExecutionEnvironment env = null;

    public SourceData(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public  DataStreamSource<String>  getData(String path){

        return env.readTextFile(path);
    }

    public DataStreamSource<String> getData(Properties Properties){

        return env.addSource(new FlinkKafkaConsumer<String>("dengjirui", new SimpleStringSchema(), Properties));

    }

}
