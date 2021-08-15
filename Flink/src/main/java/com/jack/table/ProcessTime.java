package com.jack.table;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputData = env.readTextFile("F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> dataStream = inputData.map(line -> {
            String[] split = line.split(",");
             return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp()*1000L;
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 处理时间 机器时间
        //Table tableData = tableEnv.fromDataStream(dataStream,"id,timestamp.rowtime as ts,temperature,pt.proctime");
        //事件时间
        Table tableData = tableEnv.fromDataStream(dataStream,"id,timestamp,temperature,rt.rowtime");

        tableEnv.toAppendStream(tableData, Row.class).print();
        env.execute();


    }
}
