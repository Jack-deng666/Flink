package com.jack.Redis;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class ToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> intputData = env.readTextFile("src/main/resources/seasor.txt");
        DataStream<SensorReading> dataStream = intputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        dataStream.print();
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new myRedisMapper()));
        env.execute();
    }
}


class myRedisMapper implements RedisMapper<SensorReading>{
    //自定义redisMapper 新建redis表 hset sensor_temp id temperature
    @Override
    public RedisCommandDescription getCommandDescription() {
        return  new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
    }

    @Override
    public String getKeyFromData(SensorReading data) {
        return data.getId();
    }

    @Override
    public String getValueFromData(SensorReading data) {
        return data.getTempperature().toString();
    }
}