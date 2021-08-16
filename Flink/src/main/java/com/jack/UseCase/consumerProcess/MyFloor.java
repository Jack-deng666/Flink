package com.jack.UseCase.consumerProcess;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/16 19:07
 */
public class MyFloor extends ScalarFunction {
    public @DataTypeHint("TIMESTAMP(3)")
    LocalDateTime eval(
            @DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {

        return timestamp.truncatedTo(ChronoUnit.HOURS);
    }
}
