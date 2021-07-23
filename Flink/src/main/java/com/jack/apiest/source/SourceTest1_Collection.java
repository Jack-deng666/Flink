package com.jack.apiest.source;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        // 从集合读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 152365488L, 35.8),
                new SensorReading("sensor_2", 152365489L, 36.0),
                new SensorReading("sensor_3", 152365490L, 37.5)
                )
        );
        // 打印
        DataStream<Integer> dataInt = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        dataStream.print("data");
        dataInt.print("int");
        env.execute("dengjiur");


    }
}
