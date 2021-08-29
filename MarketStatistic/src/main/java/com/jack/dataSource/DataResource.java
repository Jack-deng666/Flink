package com.jack.dataSource;

import com.jack.beans.MarketUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DataResource implements SourceFunction<MarketUserBehavior> {
    private boolean Running = true;

    List<String> behaviorList = Arrays.asList("install", "download", "uninstall", "click");
    List<String> channelList = Arrays.asList("App store", "wechat", "weibo");
    Random random = new Random();
    @Override
    public void run(SourceContext<MarketUserBehavior> ctx) throws Exception {
        while(Running){
            Long userId = random.nextLong();
            String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
            String channel = channelList.get(random.nextInt(channelList.size()));
            Long timestamp = System.currentTimeMillis();
            ctx.collect(new MarketUserBehavior(userId, channel, behavior, timestamp));
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {

    }
}
