package com.jack.hotitems;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;

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
                return element.getTimeStamp();
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> pvResult = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<String, Long>("PV", 1L);
            }
        }).keyBy(0)
                .timeWindow(Time.hours(1))
                .sum(1);

        pvResult.print();

        env.execute();
    }
}
