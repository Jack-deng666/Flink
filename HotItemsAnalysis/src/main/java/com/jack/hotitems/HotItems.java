package com.jack.hotitems;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/19 19:19
 *
 * 统计一小时内的热门商品，五分钟更新一次
 */
public class HotItems {
    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputData = env.readTextFile("F:\\RoadPinnacle\\Flink\\Flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> streamData = inputData.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] field = s.split(",");

                return new UserBehavior(new Long(field[0]),
                        new Long(field[1]),
                        new Integer(field[2]), field[3], new Long(field[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimeStamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<ItemViewCount> hotItemResult = streamData
                .filter(line -> "pv".equals(line.getBehavior()))
                .keyBy("itemsId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());

        SingleOutputStreamOperator<String> topResult = hotItemResult.keyBy("windowEnd").process(new MyProcess(5));
        topResult.print();
        env.execute("热门商品");


    }

    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    public static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();

            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    public  static class MyProcess extends KeyedProcessFunction<Tuple, ItemViewCount,String>{
        private final Integer topSize;

        public MyProcess(Integer topSize) {
            this.topSize = topSize;
        }
        ListState<ItemViewCount> hotItemTopState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<ItemViewCount> itemViewCountValueStateDescriptor =
                    new ListStateDescriptor<ItemViewCount>("item-top", ItemViewCount.class);
            hotItemTopState = getRuntimeContext().getListState(itemViewCountValueStateDescriptor);
        }


        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发 收集到当前的全部数据，进行排序
            Iterator<ItemViewCount> iterator = hotItemTopState.get().iterator();
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(iterator);
            // 排序
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue()-o1.getCount().intValue();
                }
            });
            // 将排序结果转换为String
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===========================================\n");
            resultBuilder.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");
            // 便利取出排序结果
            for(int i = 0; i<Math.min(resultBuilder.length(), topSize);i++){
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ")
                                .append(i+1)
                                .append(": 商品 = ")
                                .append(itemViewCount.getItemId())
                        .append(";  热门度:").append(itemViewCount.getCount()).append("\n");
            }
            resultBuilder.append("===========================================\n\n");

            out.collect(resultBuilder.toString());

            // 控制频率
            Thread.sleep(1000L);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            hotItemTopState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }
    }

}
