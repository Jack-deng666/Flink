package com.jack.operateDemo;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class rolllingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("src/main/resources/seasor.txt");

//        SingleOutputStreamOperator<SensorReading> mapValues = dataStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] field = s.split(",");
//                return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
//            }
//        });
        DataStream<SensorReading> mapValues = dataStream.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        // 滚动聚合
        KeyedStream<SensorReading, Tuple> KeyedStream = mapValues.keyBy("id");
//        SingleOutputStreamOperator<SensorReading> tempperature = id.maxBy("tempperature");
//        tempperature.print();

//        SingleOutputStreamOperator<SensorReading> reduceValues = KeyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading t0, SensorReading t1) throws Exception {
//                return new SensorReading(t1.getId(), t1.getTimestamp(), (double) Math.max(t0.getTimestamp(), t1.getTimestamp()));
//            }
//        });
        SingleOutputStreamOperator<SensorReading> reduceValues = KeyedStream.reduce((t0, t1) -> {
            return new SensorReading(t0.getId(),t1.getTimestamp(), (double) Math.max(t0.getTimestamp(), t1.getTimestamp()));
        });
        reduceValues.print();
        env.execute();
//        KeyedStream<SensorReading, Tuple> id = mapValues.keyBy("id");
//        SingleOutputStreamOperator<SensorReading> tempperature = id.maxBy("tempperature");
//        tempperature.print();
//        env.execute();
    }
}
