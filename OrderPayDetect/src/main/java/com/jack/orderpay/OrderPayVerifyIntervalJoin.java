package com.jack.orderpay;

import com.jack.beans.OrderEvent;
import com.jack.beans.PayEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/9/1 14:18
 */
public class OrderPayVerifyIntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 数据流1
        URL resource = OrderPayVerify.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderData= env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                }).filter(data->"pay".equals(data.getEventType()));

        // 数据流2
        URL resource1 = OrderPayVerify.class.getResource("/ReceiptLog.csv");

//        System.out.println(resource1.getPath());
        SingleOutputStreamOperator<PayEvent> payData = env.readTextFile("F:\\LoadPinnacle\\Flink\\OrderPayDetect\\src\\main\\resources\\ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new PayEvent(fields[0], fields[1], new Long(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<PayEvent>() {
                    @Override
                    public long extractAscendingTimestamp(PayEvent element) {
                        return element.getTimeStamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<Tuple2<OrderEvent, PayEvent>> process = orderData.keyBy(OrderEvent::getTxId)
                .intervalJoin(payData.keyBy(PayEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5)) // 时间区间  下限是距离pay3分钟，上限是距离pay5分钟
                .process(new JoinProcessFun());
        process.print();
        env.execute();

    }

    public static class JoinProcessFun extends ProcessJoinFunction<OrderEvent, PayEvent, Tuple2<OrderEvent, PayEvent>>{
        @Override
        public void processElement(OrderEvent left, PayEvent right, Context ctx, Collector<Tuple2<OrderEvent, PayEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left, right));
        }
    }
}
