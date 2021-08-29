package com.jack.marketanalysis;

import com.jack.beans.ChannelPromotionCount;
import com.jack.beans.MarketUserBehavior;
import com.jack.dataSource.DataResource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception{
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
                .keyBy(new KeySelector<MarketUserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(MarketUserBehavior value) throws Exception {
                        return new Tuple2<>(value.getChannel(), value.getBehavior());
                    }
                })
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new marketAggFun(), new marketWinFUN());

        aggregate.print();
        env.execute();
    }

    public static class marketAggFun implements AggregateFunction<MarketUserBehavior,Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
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

    public static class marketWinFUN extends ProcessWindowFunction<Long, ChannelPromotionCount, Tuple2<String,String>, TimeWindow>{

        @Override
        public void process(Tuple2<String,String> tuple2, Context context, Iterable<Long> elements, Collector<ChannelPromotionCount> out) throws Exception {
            String channel = tuple2.getField(0);
            String behavior = tuple2.getField(1);
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            out.collect(new ChannelPromotionCount(channel, behavior, windowEnd, elements.iterator().next()));
        }
    }
}
