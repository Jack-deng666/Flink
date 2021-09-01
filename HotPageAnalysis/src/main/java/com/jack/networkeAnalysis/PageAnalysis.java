package com.jack.networkeAnalysis;

import com.jack.HotPageCount;
import com.jack.UserView;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class PageAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = PageAnalysis.class.getResource("/apache.log");
        DataStreamSource<String> inputData = env.readTextFile(resource.getPath());

//        DataStreamSource<String> inputData = env.socketTextStream("10.0.0.22", 50000);
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
//        dataStream.print("data");

        OutputTag<UserView> hotPageCountOutputTag = new OutputTag<UserView>("late-data"){};

        SingleOutputStreamOperator<HotPageCount> aggResult = dataStream
                .filter(line->"GET".equals(line.getMethod()))
                .keyBy(UserView::getUrl)
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(hotPageCountOutputTag)
                .aggregate(new MyAgg(), new MyWin());
        aggResult.print("agg");
        aggResult.getSideOutput(hotPageCountOutputTag).print("late-data");

        SingleOutputStreamOperator<String> dataResult = aggResult
                .keyBy(HotPageCount::getWindowEnd)
                .process(new ProFun(3));

        dataResult.print();
        env.execute("hot pages job");
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
//        ListState<HotPageCount> listState;
        MapState<String, Integer> pageViewCountMapState;
        @Override
        public void open(Configuration parameters) throws Exception {
//            ListStateDescriptor<HotPageCount> hotPageCountListStateDescriptor =
//                    new ListStateDescriptor<>("list-state", HotPageCount.class);
//            listState = getRuntimeContext().getListState(hotPageCountListStateDescriptor);

            pageViewCountMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Integer>("page-map-state",String.class, Integer.class));

        }

        @Override
        public void processElement(HotPageCount value, Context ctx, Collector<String> out) throws Exception {
            pageViewCountMapState.put(value.getUrl(), value.getCount());
//            listState.add(value);
            // 输出表单定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
            // 注册清空状态定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+ 60 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            /*
            在多定时器的情况下， 判断执行那个定时器的方法是根据定时器时间，
            所以这里 两个定时器区分的是 ： 判断时间

             */
            // windowEnd ： 我们最后一个窗口是对windowEnd分组， 过意这个直接getCurrentKey即可
            //如果60s定时器 直接情况数据
            if (timestamp == ctx.getCurrentKey()+60*1000L){
              pageViewCountMapState.clear();
              return;
            }

            ArrayList<Map.Entry<String, Integer>> pageViewCount = Lists.newArrayList(pageViewCountMapState.entries().iterator());

            pageViewCount.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("=========================================\n");
            stringBuilder.append("当前窗口时间为").append(new Timestamp(timestamp-1)).append("\n");

            for(int i = 0; i<Math.min(pageViewCount.size(),TopSize);i++){
                Map.Entry<String, Integer> currentItemViewCount = pageViewCount.get(i);
                (stringBuilder.append("NO: Top").append(i + 1).append(" url :")
                        .append(currentItemViewCount.getKey()))
                        .append("; 游览量为:")
                        .append(currentItemViewCount.getValue()).append("\n");
            }
            stringBuilder.append("=========================================\n\n");
//            Thread.sleep(1000);
            out.collect(stringBuilder.toString());
//            pageViewCountMapState.clear();
        }
    }
}
