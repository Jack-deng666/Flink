package com.jack.hotitems;

import com.jack.beans.UserBehavior;
import com.jack.connect.ConnectDescribe;
import com.jack.sourceData.SourceData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 18:25
 */
public class HotItemsSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SourceData SourceData = new SourceData(env);
        // 获取文件数据
        DataStreamSource<String> inputData = SourceData.getData(ConnectDescribe.getFileProperties());

        // pojo数据
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

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // 将Stream转换成table
//        Table tableData = tableEnv.fromDataStream(streamData, "itemsId, behavior, timeStamp.rowtime as ts");
//
//        Table windowAggTable = tableData
//                .filter("behavior='pv'")
//                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
//                .groupBy("itemsId,w")
//                .select("itemsId, w.end as windowEnd, itemsId.count as cnt");
//
//        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
//        tableEnv.createTemporaryView("agg", aggStream, "itemsId, windowEnd, cnt");
//
//        Table resultData = tableEnv.sqlQuery("select * from " +
//                "       ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg)" +
//                "where row_num <= 5");


        /**
         * 全部用sql实现
         * ========================================================================================
         */
        tableEnv.createTemporaryView("agg", streamData, "itemsId, behavior, timeStamp.rowtime as ts");
        Table resultData = tableEnv.sqlQuery(
                "select " +
                "      *" +
                "from (" +
                "      select " +
                "          * ,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                "      from (" +
                "          select " +
                "              itemsId, count(itemsId) as cnt, HOP_END(ts, interval '5' minute ,interval '1' hour) as windowEnd" +
                "          from " +
                "              agg " +
                "          where " +
                "              behavior='pv' group by itemsId, HOP(ts, interval '5' minute ,interval '1' hour)" +
                "          )" +
                "     )" +
                "where " +
                "     row_num<=5");


        /**
         * ========================================================================================
         */



        tableEnv.toRetractStream(resultData, Row.class).print();

        env.execute();



    }
}
