package com.jack.networkeAnalysis;

import com.jack.beans.UserView;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.text.SimpleDateFormat;

public class PageAnalysisSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


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

//        Table tableData = tableEnv.fromDataStream(dataStream, "url, timeStamp.rowtime as ts, method");
//
//        Table tb = tableData
//                .filter("method==='GET'")
//                .window(Slide.over("10.minutes").every("5.seconds").on("ts").as("w"))
//                .groupBy("url,w")
//                .select("url, w.end as windowEnd, url.count as cnt");
//
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(tb, Row.class);
//        tableEnv.createTemporaryView("agg", rowDataStream, "url, windowEnd, cnt");
//
//        Table sqlData = tableEnv.sqlQuery("select * from " +
//                "(select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num  from agg)" +
//                "where row_num<5");
        tableEnv.createTemporaryView("agg", dataStream, "url, method, timeStamp.rowtime as ts");
        Table sqlData = tableEnv.sqlQuery("select * from " +
                                             "(" +
                                              "select " +
                                                    "*,ROW_NUMBER() over (partition by windowEnd order by cnt  desc) as row_num " +
                                              "from" +
                                                    "(select " +
                                                        "url, count(url) as cnt, HOP_END(ts, interval '5' second, interval '10' minute) as windowEnd " +
                                                    "from " +
                                                        "agg " +
                                                    "where " +
                                                        "`method`='GET' " +
                                                    "group by url, HOP(ts, interval '5' second, interval '10' minute)" +
                                                     ")" +
                                              ") " +
                                          "where row_num<5");


        tableEnv.toRetractStream(sqlData, Row.class).print();



        env.execute();


    }
}
