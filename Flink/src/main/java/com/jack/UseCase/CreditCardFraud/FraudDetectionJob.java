package com.jack.UseCase.CreditCardFraud;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 15:04 ConsumerId
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputData = env.socketTextStream("10.0.0.22", 50000);
        SingleOutputStreamOperator<ConsumerInfo> dataStream = inputData.map(line -> {
            String[] fields = line.split(",");
            return new ConsumerInfo(fields[1], new Long(fields[1]), new Double(fields[2]));
        });
        // 正常账户
        OutputTag<ConsumerMark> normalOutput = new OutputTag<ConsumerMark>("normal-consumer"){
        };

        SingleOutputStreamOperator<ConsumerMark> consumeData = dataStream.
                keyBy("ConsumerId")
                .process(new FraudDetector(normalOutput));

        consumeData.addSink(new ToMysql());
        consumeData.getSideOutput(normalOutput).addSink(new ToMysql());

        env.execute();
    }
}
