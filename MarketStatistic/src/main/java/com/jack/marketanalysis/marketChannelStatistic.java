package com.jack.marketanalysis;

import com.jack.beans.ChannelPromotionCount;
import com.jack.beans.MarketUserBehavior;
import com.jack.dataSource.DataResource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

public class marketChannelStatistic {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketUserBehavior> inputData = env.addSource(new DataResource()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketUserBehavior>() {
            @Override
            public long extractAscendingTimestamp(MarketUserBehavior element) {
                return element.getTimestamp();
            }
        });
        SingleOutputStreamOperator<ChannelPromotionCount> aggregate = inputData
                .filter(data -> !"uninstall".equals(data.getBehavior()))
                .map(new MapFunction<MarketUserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(MarketUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(),1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new aggFun1(), new winFun1());
        aggregate.print();
        env.execute();

    }

    public static class aggFun1 implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String,Integer> value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }

    public static class winFun1 implements WindowFunction<Integer, ChannelPromotionCount, String, TimeWindow>{
        @Override
        public void apply(String s , TimeWindow window, Iterable<Integer> input, Collector<ChannelPromotionCount> out) throws Exception {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            out.collect(new ChannelPromotionCount(s,"install", windowEnd, Long.valueOf(input.iterator().next())));
        }
    }
}
