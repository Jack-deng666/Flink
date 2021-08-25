package com.jack.hotitems;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Random;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/25 19:22
 */
public class PvCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(12);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = PvCount.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputData =  env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<UserBehavior> dataStream = inputData.map(line -> {
            String[] field = line.split(",");

            return new UserBehavior(new Long(field[0]),
                    new Long(field[1]),
                    new Integer(field[2]), field[3], new Long(field[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimeStamp()*1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<Integer, Long>> mapData = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        Random random = new Random();
                        return new Tuple2<Integer, Long>(random.nextInt(10), 1L);
                    }
                });

        SingleOutputStreamOperator<ItemViewCount> aggregateResult = mapData
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new MyAgg(), new MyWinFun());
        SingleOutputStreamOperator<ItemViewCount> process = aggregateResult
                .keyBy(ItemViewCount::getWindowEnd)
                .process(new MyProFun());

        process.print();

        env.execute();
    }
    public static class MyAgg implements AggregateFunction<Tuple2<Integer,Long>,Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    public static class MyWinFun implements WindowFunction<Long, ItemViewCount,Integer, TimeWindow>{
        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Random random = new Random();
            out.collect(new ItemViewCount((long) random.nextInt(10),window.getEnd(), input.iterator().next()));
        }
    }

    public static class MyProFun extends KeyedProcessFunction<Long,ItemViewCount,ItemViewCount>{


        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("total-count",Long.class,0L));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<ItemViewCount> out) throws Exception {
            Long totalCount = valueState.value() + value.getCount();
            valueState.update(totalCount);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ItemViewCount> out) throws Exception {
            Long  totalCount = valueState.value();
            Random random = new Random();
            out.collect(new ItemViewCount((long) random.nextInt(10), ctx.getCurrentKey(), totalCount));
            this.valueState.clear();
        }
    }
}
