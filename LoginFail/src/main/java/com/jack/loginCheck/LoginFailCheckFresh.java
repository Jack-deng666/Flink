package com.jack.loginCheck;

import com.jack.beans.LoginBehavior;
import com.jack.beans.LoginFailBehavior;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Iterator;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 16:31
 */
public class LoginFailCheckFresh {
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
                inputData.keyBy(LoginBehavior::getUserId).process(new MyProcessFunFresh());

        process.print();
        env.execute();
    }

    public static  class MyProcessFunFresh extends KeyedProcessFunction<Long, LoginBehavior, LoginFailBehavior>{


        ListState<LoginBehavior> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<LoginBehavior>("listState",LoginBehavior.class));
        }


        @Override
        public void processElement(LoginBehavior value, Context ctx, Collector<LoginFailBehavior> out) throws Exception {
            // 登录失败
            if("fail".equals(value.getLoginState())){
                Iterator<LoginBehavior> iterator = listState.get().iterator();
                // 判断是否前面有失败
                if (iterator.hasNext()){

                    LoginBehavior firstLogin = iterator.next();
                    // 判断两次失败的时间差是不是2s  如果时间差在2s，触发报警，如果两次的时间差大于2s，更新状态。
                    if(value.getTimestamp()-firstLogin.getTimestamp()<=2){
                        out.collect(new LoginFailBehavior(ctx.getCurrentKey(),firstLogin.getTimestamp(),value.getTimestamp(),"报警:异常登陆 2 times"));
                    }
                    listState.clear();
                    listState.add(value);
                }
                listState.add(value);
            }
            // 登陆成功 清空状态
            else{
                listState.clear();
            }
        }
    }
}

