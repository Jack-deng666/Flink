package com.jack.state.keydState;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TemperatureWarn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputData = env.socketTextStream("192.168.254.129", 50000);
        DataStream<SensorReading> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warn =
                dataStream.keyBy("id").flatMap(new MyRichFlatmapFun(10.0));
        warn.print("warnTemp");
        env.execute();
    }

    public static class MyRichFlatmapFun extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{
        private Double Warnthreshold;
        private ValueState<Double> lastTempSate;

        // 构造器
        public MyRichFlatmapFun(Double warnthreshold) {
            Warnthreshold = warnthreshold;
        }
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempSate = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temperature",Double.class));
        }
        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempSate.value();
            if (lastTemp != null){
                Double tempDiff = Math.abs(value.gettemperature() - lastTemp);
                if (tempDiff >= Warnthreshold){
//                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.gettemperature()));
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.gettemperature()));
                    }
                }
            // 更新状态
            lastTempSate.update(value.gettemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempSate.clear();
        }
    }
}
