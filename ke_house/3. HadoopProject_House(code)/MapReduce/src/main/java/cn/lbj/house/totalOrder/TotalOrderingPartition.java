package cn.lbj.house.totalOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class TotalOrderingPartition extends Configured implements Tool {

    static class SimpleMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            String total = value.toString().split("\t")[6];
//            System.out.println(total);
//            IntWritable intWritable = new IntWritable(Integer.parseInt(total));
            IntWritable intWritable = new IntWritable(Integer.parseInt(key.toString()));
            context.write((Text) key, intWritable);
        }
    }

    static class SimpleReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Total Order Sorting");
        job.setJarByClass(TotalOrderingPartition.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler<Text, Text>(10000, 10);
        InputSampler.writePartitionFile(job, sampler);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(SimpleMapper.class);
        job.setReducerClass(SimpleReducer.class);

        job.setJobName("TotalOrderingPartition");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[] { "datas/tb_house_total.txt", "MapReduce/out/TotalOrderingPartition/outPartition", "MapReduce/out/TotalOrderingPartition/outPartitionFile" };
        int exitCode = ToolRunner.run(new TotalOrderingPartition(), args);
        System.exit(exitCode);
    }

}
