package com.jack.table.udfFunction;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;

public class tableAggFun {
    public static void main(String[] args) {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        // 流式读取数据
        DataStreamSource<String> inputData = env.readTextFile("F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> StreamData = inputData.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        tableAggregate tableAggregate = new tableAggregate();
        tableEnv.registerFunction("tempTop",tableAggregate);
        // 流式数据转成表
        Table tableData = tableEnv.fromDataStream(StreamData, "id, timestamp,temperature");
        Table id = tableData.groupBy("id")
                .flatAggregate("tempTop(temperature) as temp1, temp2")
                .select("id, temp1, temp2");
        tableEnv.toRetractStream(id, Row.class).print();

    }
    public static class tableAggregate extends TableAggregateFunction<Tuple2<Double,Double>, HashMap<String, Double>>{

        @Override
        public HashMap<String, Double> createAccumulator() {
            HashMap<String, Double> hash = new HashMap<>();
            hash.put("tempTop1",0.0);
            hash.put("tempTop2",0.0);
            return hash;
        }

        public  static void accumulate(HashMap<String, Double> hashMap, Double temp){
            Double tempTop1 = hashMap.get("tempTop1");
            Double tempTop2 = hashMap.get("tempTop2");
            if(temp>tempTop1){
                hashMap.replace("tempTop1", temp);
            }
            else  if (temp>tempTop2){
                hashMap.replace("tempTop2", temp);
            }
        }
        public static void emitValue(HashMap<String, Double> hashMap, Tuple2<Double,Double> out){
            out.f0 = hashMap.get("tempTop1");
            out.f1 = hashMap.get("tempTop2");
        }
    }
}
