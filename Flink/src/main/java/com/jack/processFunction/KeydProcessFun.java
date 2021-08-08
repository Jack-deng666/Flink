package com.jack.processFunction;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class KeydProcessFun {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputData = env.socketTextStream("192.168.254.129", 50000);
        DataStream<SensorReading> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        dataStream.keyBy("id").process(new MyKeydProcessFunction()).print();

        env.execute();
    }

    public static class MyKeydProcessFunction extends KeyedProcessFunction<Tuple, SensorReading, Integer> {
        ValueState<Long> TsTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            TsTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-Time", Long.class));
        }

        @Override
        public void close() throws Exception {
            TsTimeState.clear();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            ctx.getCurrentKey();
            ctx.timerService();
            ctx.timestamp();
            // 在做定时时候，可以暂时保存开始任务的事件
            TsTimeState.update(ctx.timerService().currentProcessingTime()+1000L);
            // 定时任务
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000L);
//            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime()+5000L);
//            ctx.timerService().deleteProcessingTimeTimer(TsTimeState.value()+6000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp+ "定时器触发");
        }
    }
}
