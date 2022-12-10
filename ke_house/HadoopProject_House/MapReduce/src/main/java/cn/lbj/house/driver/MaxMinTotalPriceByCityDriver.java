package cn.lbj.house.driver;

import cn.lbj.house.mapper.MaxMinTotalPriceByCityMapper;
import cn.lbj.house.reducer.MaxMinTotalPriceByCityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxMinTotalPriceByCityDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MaxMinTotalPriceByCity");
        job.setJarByClass(MaxMinTotalPriceByCityDriver.class);
        job.setMapperClass(MaxMinTotalPriceByCityMapper.class);
        job.setReducerClass(MaxMinTotalPriceByCityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("datas/tb_house.txt"));
        FileOutputFormat.setOutputPath(job, new Path("MapReduce/out/MaxMinTotalPriceByCity"));


        job.waitForCompletion(true);
    }
}
