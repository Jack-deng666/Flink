package com.jack.orderpay;

import com.jack.beans.OrderEvent;
import com.jack.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class OrderTimeOutProcess {

    private  static OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("timeout"){};


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = OrderTimeOut.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> inputData = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<OrderResult> process = inputData.keyBy(OrderEvent::getOrderId)
                .process(new MyProFun());
        process.print("pay successful order");
        process.getSideOutput(outputTag).print("timeout pay order");
        env.execute();

    }

    public static class MyProFun extends KeyedProcessFunction<Long, OrderEvent, OrderResult>{
        ValueState<Long> valueState;
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("order state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            if("create".equals(value.getEventType())){

                Long ts = (value.getTimestamp()+15*60)*1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                valueState.update(ts);
            }
            else{
                if(valueState.value() != null){
                    if(valueState.value()<value.getTimestamp()*1000L){
//                        ctx.output(outputTag,new OrderResult(ctx.getCurrentKey(), "订单支付超时"+value.getTimestamp()*1000L));
                        valueState.clear();
                    }
                    else{
                        ctx.timerService().deleteEventTimeTimer(valueState.value());
                        valueState.clear();
                        out.collect(new OrderResult(ctx.getCurrentKey(), "订单支付成功"));
                    }
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            ctx.output(outputTag,new OrderResult(ctx.getCurrentKey(), "订单支付超时"+timestamp));
        }
    }
}
