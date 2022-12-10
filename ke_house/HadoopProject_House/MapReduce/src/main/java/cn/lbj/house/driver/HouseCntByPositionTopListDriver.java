package cn.lbj.house.driver;

import cn.lbj.house.mapper.HouseCntByPositionTopListMapper;
import cn.lbj.house.reducer.HouseCntByPositionTopListReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HouseCntByPositionTopListDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "HouseCntByPositionTopList");
        job.setJarByClass(HouseCntByPositionTopListDriver.class);
        job.setMapperClass(HouseCntByPositionTopListMapper.class);
        job.setReducerClass(HouseCntByPositionTopListReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("datas/tb_house.txt"));
        FileOutputFormat.setOutputPath(job, new Path("MapReduce/out/HouseCntByPositionTopList"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
