package com.lbj.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 北京/上海/深圳/成都/武汉/厦门
 * 北京/上海/深圳
 *
 * 北京   2
 * 上海   2
 * 深圳   2
 * 成都   1
 * 武汉   1
 * 厦门   1
 * @date：Created in 22:25 2022/6/8
 */


public class GetAddrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 添加计数器
        context.getCounter("MapTask", "setup").increment(1);
//        System.out.println("setup()方法执行了");
    }

    /**
     *  Map阶段的核心业务处理方法，每输入一行数据会调用一次map方法
     * @param key   输入数据的key
     * @param value 输入数据的value
     * @param context   上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 添加计数器
        context.getCounter("MapTask", "map").increment(1);

        // 2. 获取当前输入的数据
        String line = value.toString();

        // 3. 切割数据
        String[] datas = line.split("/");

        // 4. 遍历集合 封装输出数据的key和value
        for (String data : datas) {
            outk.set(data);
            context.write(outk, outv);
        }
//        System.out.println("map()方法执行了");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 添加计数器
        context.getCounter("MapTask", "cleanup").increment(1);

//        System.out.println("cleanup()方法执行了");
    }
}
