package com.lbj.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @date：Created in 22:25 2022/6/8
 */
public class GetAddrReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable();


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int total = 0;

        // 遍历 values
        for (IntWritable value : values) {
            // 累加value 输出结果
            total += value.get();
        }
        // 封装key和value
        outk.set(key);
        outv.set(total);
        context.write(outk, outv);
    }
}
