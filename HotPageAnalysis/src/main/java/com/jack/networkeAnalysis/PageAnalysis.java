package com.jack.networkeAnalysis;

import com.jack.beans.HotPageCount;
import com.jack.beans.UserView;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;

public class PageAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = PageAnalysis.class.getResource("/apache.log");
        DataStreamSource<String> inputData = env.readTextFile(resource.getPath());
        SingleOutputStreamOperator<UserView> dataStream = inputData.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/yy/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();

            return new UserView(fields[0], fields[1], fields[6], timestamp, fields[5]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserView>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserView element) {
                return element.getTimeStamp();
            }
        });
//        dataStream.print();
        SingleOutputStreamOperator<HotPageCount> aggResult = dataStream
                .filter(line->"GET".equals(line.getMethod()))
                .keyBy(UserView::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .aggregate(new MyAgg(), new MyWin());
//        aggResult.print();
        SingleOutputStreamOperator<String> dataResult = aggResult.keyBy(HotPageCount::getWindowEnd)
                .process(new ProFun(3));

        dataResult.print();
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<UserView, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserView value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class MyWin implements WindowFunction<Integer, HotPageCount, String, TimeWindow> {

        @Override
        public void apply(String url, TimeWindow window, Iterable<Integer> input, Collector<HotPageCount> out) throws Exception {
            out.collect(new HotPageCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    public static class  ProFun extends KeyedProcessFunction<Long, HotPageCount, String> {

        private final Integer TopSize;

        public ProFun(Integer topSize) {
            TopSize = topSize;
        }
        ListState<HotPageCount> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<HotPageCount> hotPageCountListStateDescriptor =
                    new ListStateDescriptor<>("list-state", HotPageCount.class);
            listState = getRuntimeContext().getListState(hotPageCountListStateDescriptor);
        }

        @Override
        public void processElement(HotPageCount value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<HotPageCount> hotPageCounts = Lists.newArrayList(listState.get().iterator());
            hotPageCounts.sort(new Comparator<HotPageCount>() {
                @Override
                public int compare(HotPageCount o1, HotPageCount o2) {
                    // 大于-1 表示不作操作
                    return o2.getCount().compareTo(o1.getCount());
                }
            });
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=========================================\n");
            stringBuilder.append("当前窗口时间为").append(timestamp-1).append("\n");
            int i = 0;
            for(i++; i<Math.min(hotPageCounts.size(),TopSize);i++){
                HotPageCount hotPageCount = hotPageCounts.get(i);
                (stringBuilder.append("NO: Top").append(i + 1).append(" url :")
                        .append(hotPageCount.getUrl()))
                        .append("; 游览量为:")
                        .append(hotPageCount.getCount()).append("\n");
            }
            stringBuilder.append("=========================================\n\n");

            out.collect(stringBuilder.toString());
            Thread.sleep(1000);

        }
    }
}
