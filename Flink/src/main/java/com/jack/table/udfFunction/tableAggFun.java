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
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;

/**
 * 参照官网的例子：https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/dev/table/tableapi/
 */
public class tableAggFun {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        // 流式读取数据
        DataStreamSource<String> inputData = env.readTextFile("F:\\LoadPinnacle\\Flink\\Flink\\src\\main\\resources\\sensor.txt");
        SingleOutputStreamOperator<SensorReading> StreamData = inputData.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        tableAggregate tableAggregate = new tableAggregate();
        tableEnv.registerFunction("tempTop",tableAggregate);
        // 流式数据转成表
        Table tableData = tableEnv.fromDataStream(StreamData, "id, timestamp,temperature");
        Table id = tableData.groupBy("id")
                .flatAggregate("tempTop(temperature) as (tempOne, rank)")
                .select("id, tempOne, rank");
        tableEnv.toRetractStream(id, Row.class).print();
        env.execute();

    }
        public static class tableAggregate extends TableAggregateFunction<Tuple2<Double,Integer>, HashMap<String, Double>>{

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
            else if(temp>tempTop2){
                hashMap.replace("tempTop2", temp);
            }
        }

        public void merge(HashMap<String, Double> hash,java.lang.Iterable<HashMap<String, Double>> iterable){
                for(HashMap<String, Double> h: iterable){
                    accumulate(hash, h.get("tempTop1"));
                    accumulate(hash, h.get("tempTop2"));
                }
        }
        public static void emitValue(HashMap<String, Double> hashMap, Collector<Tuple2<Double, Integer>> out){
            if (hashMap.get("tempTop1") != 0.0){
                out.collect(new Tuple2<>(hashMap.get("tempTop1"), 1));
            }
            if (hashMap.get("tempTop2") != 0.0){
                out.collect(new Tuple2<>(hashMap.get("tempTop2"), 2));
            }
        }
    }
}
