package com.jack.loginCheck;

import com.jack.beans.LoginBehavior;
import com.jack.beans.LoginFailBehavior;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 17:55
 */
public class LoginFailCepFresh {

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
        // 定义匹配模式
        Pattern<LoginBehavior, LoginBehavior> loginFail = Pattern.<LoginBehavior>begin("loginFail").where(new SimpleCondition<LoginBehavior>() {
            @Override
            public boolean filter(LoginBehavior value) throws Exception {

                return "fail".equals(value.getLoginState());
            }
        }).times(3).consecutive();

        SingleOutputStreamOperator<LoginFailBehavior> select = CEP.pattern(inputData.keyBy(LoginBehavior::getUserId), loginFail).select(new MyPatternSelectFun());
        select.print();
        env.execute();
    }


    public static class MyPatternSelectFun implements PatternSelectFunction<LoginBehavior, LoginFailBehavior> {

        @Override
        public LoginFailBehavior select(Map<String, List<LoginBehavior>> pattern) throws Exception {

            LoginBehavior loginFail = pattern.get("loginFail").get(0);
            LoginBehavior loginFail1 = pattern.get("loginFail").get(pattern.size() - 1);
            return new LoginFailBehavior(loginFail1.getUserId(), loginFail.getTimestamp(), loginFail1.getTimestamp(),"登陆失败预警两次");
        }
    }
}
