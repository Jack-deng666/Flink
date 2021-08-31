package com.jack.loginCheck;

import com.jack.beans.LoginBehavior;
import com.jack.beans.LoginFailBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Int;

import java.net.URL;
import java.util.ArrayList;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 15:33
 */
public class LoginFailCheck {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = LoginFailCheck.class.getResource("/LoginLog.csv");
        DataStream<LoginBehavior> inputData = env.readTextFile(resource.getPath()).map(line -> {
            String[] fields = line.split(",");
            return new LoginBehavior(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginBehavior>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(LoginBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<LoginFailBehavior> process =
                inputData.keyBy(LoginBehavior::getUserId).process(new MyProcessFun(3));

        process.print();
        env.execute();

    }

    public static class MyProcessFun extends KeyedProcessFunction<Long, LoginBehavior, LoginFailBehavior>{

        private Integer maxFailLoginTime;

        public MyProcessFun(Integer maxFailLoginTime) {
            this.maxFailLoginTime = maxFailLoginTime;
        }

        ValueState<Long> isFailExitState;
        ListState<LoginBehavior> LoginFailTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            LoginFailTimeState = getRuntimeContext().getListState(new ListStateDescriptor<LoginBehavior>("login fail times", LoginBehavior.class));
            isFailExitState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("login fail record", Long.class));
        }

        @Override
        public void processElement(LoginBehavior value, Context ctx, Collector<LoginFailBehavior> out) throws Exception {
            if("fail".equals(value.getLoginState())){
                if (isFailExitState.value() == null){
                    Long ts = (value.getTimestamp()+2)*1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    isFailExitState.update(ts);
                }
                LoginFailTimeState.add(value);
            }
            else{
                if (null != isFailExitState.value()) {
                    ctx.timerService().deleteEventTimeTimer(isFailExitState.value());
                }
                LoginFailTimeState.clear();
                isFailExitState.clear();
            }
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailBehavior> out) throws Exception {
            ArrayList<LoginBehavior> loginBehaviors = Lists.newArrayList(LoginFailTimeState.get().iterator());
            int size = loginBehaviors.size();
            if(size>=maxFailLoginTime){
                out.collect(new LoginFailBehavior(
                        new Long(ctx.getCurrentKey()),
                        new Long(loginBehaviors.get(0).getTimestamp()),
                        new Long(loginBehaviors.get(size-1).getTimestamp())
                        ,"报警:异常登陆 "+size+" times"));
            }
            LoginFailTimeState.clear();
            isFailExitState.clear();
        }
    }
}
