package com.jack.orderpay;
import com.jack.beans.OrderEvent;
import com.jack.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 19:23
 */
public class OrderTimeOut {
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

        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> pattern1 = CEP.pattern(inputData.keyBy(OrderEvent::getOrderId), pattern);

        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("timeout"){};

        SingleOutputStreamOperator<OrderResult> select = pattern1.select(outputTag, new TimeOutFun(), new OrderPayFun());

        select.print("payed normally");
        select.getSideOutput(outputTag).print("timeout");
        env.execute();

    }

    public static class TimeOutFun  implements PatternTimeoutFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            OrderEvent values = pattern.get("create").iterator().next();
            return new OrderResult(values.getOrderId(), "订单支付超时"+timeoutTimestamp);
        }
    }

    public static class OrderPayFun implements PatternSelectFunction<OrderEvent, OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
            return new OrderResult(pattern.get("pay").iterator().next().getOrderId(), "订单已经支付");
        }
    }

}
