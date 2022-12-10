package cn.lbj.house.driver;

import cn.lbj.house.mapper.TagRatioByCityMapper;
import cn.lbj.house.reducer.TagRatioByCityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TagRatioByCityDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] {"datas/tb_house.txt", "MapReduce/out/TagRatioByCity" };

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TagRatioByCityDriver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TagRatioByCityMapper.class);
        job.setReducerClass(TagRatioByCityReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
