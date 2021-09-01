package com.jack.Uv;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;


public class UniqueVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputData = env.readTextFile(resource.getPath());
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

        SingleOutputStreamOperator<ItemViewCount> resultData = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .apply(new MyApply());

        resultData.print();
        env.execute();
    }

    public static class MyApply implements AllWindowFunction<UserBehavior, ItemViewCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<ItemViewCount> out) throws Exception {
            HashSet<Long> userBehaviors = new HashSet<>();
            for(UserBehavior us:values){
                userBehaviors.add(us.getUserId());
            }
            out.collect(new ItemViewCount(1L, window.getEnd(), (long)userBehaviors.size()));
        }
    }
}
