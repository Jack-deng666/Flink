package com.jack.Uv;

import com.jack.beans.ItemViewCount;
import com.jack.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.List;

public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStreamSource<String> inputData = env.readTextFile(resource.getPath());
        SingleOutputStreamOperator<UserBehavior> dataStream = inputData.map(line -> {
            String[] field = line.split(",");

            return new UserBehavior(new Long(field[0]),
                    new Long(field[1]),
                    new Integer(field[2]), field[3], new Long(field[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimeStamp()*1000L;
            }
        });
        SingleOutputStreamOperator<ItemViewCount> process = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger()) //自定义定时器状态
                .process(new MyProcessCount());

        process.print();

        env.execute();

    }

    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        // 有数据过来
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE; //返回一个枚举类型 （boolean，boolean）（计算，清空）
        }


        // 处理时间发生变化
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        // 时间时间发生变化 即wasterMask
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }
        // 清空自定义状态
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    // 自定义boolFilter
    public static  class MyBloomFilter{
        private  Integer cap; // 位图大小

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        // 自定义hash
        public Long hashMap(String value, Integer seed){
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap-1);

        }
    }

    public static class MyProcessCount extends ProcessAllWindowFunction<UserBehavior,ItemViewCount,TimeWindow>{
        // 连接redis
        Jedis redisConn;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            redisConn =  new Jedis ("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29); // 假设定义一个亿的数据,64MB位图
        }

        @Override
        public void close() throws Exception {
            redisConn.close();
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<ItemViewCount> out) throws Exception {
            Long userId = elements.iterator().next().getUserId();
            String userName = userId.toString();
            Long windowEnd = context.window().getEnd();
            String CountKey = windowEnd.toString();
            String CountHashName = "uv_count";

            Long offset = myBloomFilter.hashMap(userName, 61);

            Boolean getbit = redisConn.getbit(CountKey, offset);
            if(!getbit){
                // 不存在这个用户计入
                redisConn.setbit(CountKey,offset,true);
                Long count = 0L;
                String countKey = redisConn.hget(CountHashName, CountKey);
                if( countKey!=null & !"".equals(countKey)){
                    count = Long.valueOf(countKey);
                }
                redisConn.hset(CountHashName, CountKey, String.valueOf(count + 1));
                out.collect(new ItemViewCount(1L, windowEnd, count + 1));

            }
        }
    }
}
