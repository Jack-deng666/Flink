package com.jack.hotitems;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import com.jack.connect.ConnectDescribe;
import com.jack.sourceData.SourceData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

        SourceData SourceData = new SourceData(env);
        // 获取文件数据
//        DataStreamSource<String> inputData = SourceData.getData(ConnectDescribe.getFileProperties());
        // 获取kafka数据
        DataStreamSource<String> inputData = SourceData.getData(ConnectDescribe.getKafkaProperties());


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
                .filter(line -> "pv".equals(line.getBehavior()))        // 过滤数据
                .keyBy("itemsId")                               // 商品分组
                .timeWindow(Time.hours(1), Time.minutes(5))             // 滑动窗口
                .aggregate(new ItemCountAgg(), new WindowItemCountResult()); //增量聚合 全窗口函数

        SingleOutputStreamOperator<String> topResult = hotItemResult.keyBy("windowEnd").process(new MyProcess(5));
        topResult.print();
        env.execute("热门商品");
    }

}
