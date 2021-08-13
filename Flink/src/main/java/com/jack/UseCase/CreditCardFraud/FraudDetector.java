package com.jack.UseCase.CreditCardFraud;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 15:09
 */
public class FraudDetector extends KeyedProcessFunction<Tuple,ConsumerInfo,ConsumerMark> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;
    /**
     * transient 关键字：一些关键的信息只会存在在调用者内存的生命周期内，不会被写入磁盘持久化
     *
     *      1）一旦变量被transient修饰，变量将不再是对象持久化的一部分，该变量内容在序列化后无法获得访问。
     *
     *      2）transient关键字只能修饰变量，而不能修饰方法和类。注意，本地变量是不能被transient关键字修饰的。变量如果是用户自定义类变量，则该类需要实现Serializable接口。
     *
     *      3）被transient关键字修饰的变量不再能被序列化，一个静态变量不管是否被transient修饰，均不能被序列化。
     */
    private  transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;

    private final OutputTag<ConsumerMark> normalOutput;

    public FraudDetector(OutputTag<ConsumerMark> normalOutPut) {
        normalOutput = normalOutPut;
    }

    @Override
    public void open(Configuration parameters){
        ValueStateDescriptor<Boolean> flagStateDescribe = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagStateDescribe);

        ValueStateDescriptor<Long> timerStateDescribe = new ValueStateDescriptor<>("time",Types.LONG);
        timerState = getRuntimeContext().getState(timerStateDescribe);
    }


    @Override
    public void processElement(ConsumerInfo value, Context ctx, Collector<ConsumerMark> out) throws Exception {
        Boolean flagValue = flagState.value();
        ConsumerMark consumerMark = new ConsumerMark();
        consumerMark.setId(value.getConsumerId());
        consumerMark.setAmount(value.getAmount());
        consumerMark.setTs(value.getTs());

        if(flagValue != null){

            if(value.getAmount() > LARGE_AMOUNT){
                consumerMark.setNormal(false);
                System.out.println("糟了，诈骗");
                out.collect(consumerMark);
            }
            else{
                consumerMark.setNormal(true);
                System.out.println("哎，虚惊一场");
                ctx.output(normalOutput, consumerMark);
            }
            cleanUp(ctx);
        }

        if(value.getAmount() < SMALL_AMOUNT){
            System.out.println("准备，进入小金额");
            flagState.update(true);
            long timer = ctx.timerService().currentProcessingTime() + ONE_MINUTE;
            ctx.timerService().registerProcessingTimeTimer(timer);

            consumerMark.setNormal(true);
            ctx.output(normalOutput, consumerMark);
            timerState.update(timer);

        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConsumerMark> out){
        timerState.clear();
        flagState.clear();
    }

    private void cleanUp(Context ctx) throws IOException {
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        timerState.clear();
        flagState.clear();


    }
}
