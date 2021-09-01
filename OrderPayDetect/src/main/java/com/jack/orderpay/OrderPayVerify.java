package com.jack.orderpay;

import com.jack.beans.OrderEvent;
import com.jack.beans.PayEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.net.URL;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/9/1 10:41
 */
public class OrderPayVerify {
    public static OutputTag<OrderEvent> orderEventOutputTag= new OutputTag<OrderEvent>("make order"){};
    public static OutputTag<PayEvent> payEventOutputTag= new OutputTag<PayEvent>("make pay"){};

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

        // 将两条流进行合并处理

        SingleOutputStreamOperator<Tuple2<String, String>> process = orderData
                .keyBy(OrderEvent::getTxId)
                .connect(payData.keyBy(PayEvent::getTxId))
                .process(new MyCoProFun());


        process.print();
        process.getSideOutput(orderEventOutputTag).print("make order");
        process.getSideOutput(payEventOutputTag).print("pay order");
        env.execute();
    }

    public static class MyCoProFun extends CoProcessFunction<OrderEvent, PayEvent, Tuple2<String,String>>{
        ValueState<OrderEvent> orderState;
        ValueState<PayEvent> paySate;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order state", OrderEvent.class));
            paySate = getRuntimeContext().getState(new ValueStateDescriptor<PayEvent>("pay state", PayEvent.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            orderState.update(value);
            if(paySate.value()!=null){
                out.collect(new Tuple2<String,String>(value.getOrderId().toString(), paySate.value().getTxId()));
                orderState.clear();
                paySate.clear();
            }
            else{
                long ts = (value.getTimestamp()+3)*1000L;
                ctx.timerService().registerEventTimeTimer(ts);
            }

        }

        @Override
        public void processElement2(PayEvent value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            paySate.update(value);
            if(orderState.value()!=null){
                out.collect(new Tuple2<String,String>(orderState.value().getTxId(),value.getTxId()));
                orderState.clear();
                paySate.clear();
            }
            else{
                long ts = (value.getTimeStamp()+3)*1000L;
                ctx.timerService().registerEventTimeTimer(ts);

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if(orderState.value() != null){
                ctx.output(orderEventOutputTag, orderState.value());
            }

            if(paySate.value() != null){
                ctx.output(payEventOutputTag,paySate.value());
            }

            orderState.clear();
            paySate.clear();
        }
    }


}
