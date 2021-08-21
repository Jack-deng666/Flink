package com.jack.hotitems;

import com.jack.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 16:37
 */
public  class ItemCountAgg implements AggregateFunction<UserBehavior,Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong+acc1;
    }
}