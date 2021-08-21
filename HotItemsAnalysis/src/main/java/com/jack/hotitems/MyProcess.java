package com.jack.hotitems;

import com.jack.beans.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 16:38
 */
public  class MyProcess extends KeyedProcessFunction<Tuple, ItemViewCount,String> {
    private final Integer topSize;

    public MyProcess(Integer topSize) {
        this.topSize = topSize;
    }
    ListState<ItemViewCount> hotItemTopState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<ItemViewCount> itemViewCountValueStateDescriptor =
                new ListStateDescriptor<ItemViewCount>("item-top", ItemViewCount.class);
        hotItemTopState = getRuntimeContext().getListState(itemViewCountValueStateDescriptor);
    }


    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发 收集到当前的全部数据，进行排序
        Iterator<ItemViewCount> iterator = hotItemTopState.get().iterator();
        ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(iterator);
        // 排序
        itemViewCounts.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                return o2.getCount().intValue()-o1.getCount().intValue();
            }
        });
        // 将排序结果转换为String
        StringBuilder resultBuilder = new StringBuilder();
        resultBuilder.append("===========================================\n");
        resultBuilder.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");
        // 便利取出排序结果
        for(int i = 0; i<Math.min(itemViewCounts.size(), topSize);i++){
            ItemViewCount itemViewCount = itemViewCounts.get(i);
            resultBuilder.append("NO ")
                    .append(i+1)
                    .append(": 商品 = ")
                    .append(itemViewCount.getItemId())
                    .append(";  热门度:").append(itemViewCount.getCount()).append("\n");
        }
        resultBuilder.append("===========================================\n\n");

        out.collect(resultBuilder.toString());

        // 控制频率
//        Thread.sleep(1000L);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        hotItemTopState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }
}