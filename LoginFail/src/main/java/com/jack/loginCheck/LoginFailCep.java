package com.jack.loginCheck;

import com.jack.beans.LoginBehavior;
import com.jack.beans.LoginFailBehavior;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
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
 * @date 2021/8/31 17:41
 */
public class LoginFailCep {

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

        Pattern<LoginBehavior,LoginBehavior> loginFailPattern =  Pattern.<LoginBehavior>begin("firstLoginFail").where(new SimpleCondition<LoginBehavior>() {
            @Override
            public boolean filter(LoginBehavior value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("secondLoginFail").where(new SimpleCondition<LoginBehavior>() {
            @Override
            public boolean filter(LoginBehavior value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(2));
        PatternStream<LoginBehavior> patternStream = CEP.pattern(inputData.keyBy(LoginBehavior::getUserId), loginFailPattern);
        SingleOutputStreamOperator<LoginFailBehavior> select = patternStream.select(new MyLoginFailPatternDetect());
        select.print();
        env.execute();

    }

    public static class MyLoginFailPatternDetect implements PatternSelectFunction<LoginBehavior, LoginFailBehavior>{
        @Override
        public LoginFailBehavior select(Map<String, List<LoginBehavior>> pattern) throws Exception {
            LoginBehavior firstLoginFail = pattern.get("firstLoginFail").iterator().next();
            LoginBehavior secondLoginFail = pattern.get("secondLoginFail").iterator().next();
            return new LoginFailBehavior(firstLoginFail.getUserId(), firstLoginFail.getTimestamp(), secondLoginFail.getTimestamp(), "登录失败超时预警2次");
        }
    }
}
