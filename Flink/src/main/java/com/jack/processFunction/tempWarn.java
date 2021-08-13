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
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/9 19:46
 */
public class tempWarn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputData = env.socketTextStream("10.0.0.22", 50000);
        DataStream<SensorReading> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        dataStream.keyBy("id").
                process(new MyTempWarnProcess(5)).
                print();

        env.execute();

    }
    public static class MyTempWarnProcess extends KeyedProcessFunction<Tuple, SensorReading, String> {
        public Integer interval;
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void close() throws Exception {
            lastTempState.clear();

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-time",
                    Double.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-on", Long.class));
        }

        public MyTempWarnProcess(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();
            if(lastTemp != null && value.gettemperature() >= lastTemp && timerTs == null){
               Long ts =  ctx.timerService().currentProcessingTime() + interval*1000L;
               ctx.timerService().registerProcessingTimeTimer(ts);
               timerTsState.update(ts);
            }
            else if(lastTemp != null && value.gettemperature() < lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
            }
            lastTempState.update(value.gettemperature());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//            super.onTimer(timestamp, ctx, out);
            out.collect("传感器" + ctx.getCurrentKey().getField(0)+"发生"+interval+"s温度连续上升报警");
            timerTsState.clear();
        }


    }
}
