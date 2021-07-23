package com.jack.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        String inputPath = "D:\\java_project\\Flink_Demo\\src\\main\\resources\\hello.txt";
        DataSet<String> dataset = env.readTextFile(inputPath);
        // 对数据进行处理

        // 对数按照空格进行分集
        DataSet<Tuple2<String, Integer>> resultSet = dataset.flatMap(new myFlatMapper())
                .groupBy(0) //按照第一个位置分组;
                .sum(1).setParallelism(2); // 对第二个位置的数进行统计;
        resultSet.print();
    }

    public static class myFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            // 遍历所有的word
            for(String word: words){
                out.collect(new Tuple2<>(word, 1));

            }
        }
    }
}
