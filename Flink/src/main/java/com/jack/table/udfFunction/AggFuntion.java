package com.jack.table.udfFunction;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

public class AggFuntion {
    public static void main(String[] args) throws Exception {
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
        aggregateFun avgTemp = new aggregateFun();
        tableEnv.registerFunction("avgTemp",avgTemp);

        // 流式数据转成表
        Table tableData = tableEnv.fromDataStream(StreamData, "id, timestamp,temperature");
        Table avgTempResult = tableData.groupBy("id")
                .aggregate("avgTemp(temperature) as avgTemp")
                .select("id, avgTemp");
        tableEnv.createTemporaryView("sensor",tableData);
        Table tableSqlData = tableEnv.sqlQuery("select id, avgTemp(temperature) as avgTemp from sensor group by id");
        tableEnv.toRetractStream(tableSqlData, Row.class).print();
//        tableEnv.toRetractStream(avgTempResult, Row.class).print();
        env.execute();
    }
    public static class aggregateFun extends AggregateFunction<Double, Tuple2<Double, Integer>>{

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }
        // 自定义累加器 函数名不能变
        public void accumulate(Tuple2<Double,Integer> accumulator,Double temp){
            accumulator.f0+=temp;
            accumulator.f1+=1;
        }
    }
}
