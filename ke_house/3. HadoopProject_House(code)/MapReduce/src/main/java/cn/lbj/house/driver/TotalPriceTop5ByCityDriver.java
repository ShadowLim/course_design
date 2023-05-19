package cn.lbj.house.driver;

import cn.lbj.house.mapper.TotalPriceTop5ByCityMapper;
import cn.lbj.house.reducer.TotalPriceTop5ByCityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalPriceTop5ByCityDriver {
    public static void main(String[] args) throws Exception {
        args = new String[] {  "datas/tb_house.txt", "MapReduce/out/TotalPriceTop5ByCity" };

        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: TotalPriceTop5ByCity <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(TotalPriceTop5ByCityDriver.class);
        job.setMapperClass(TotalPriceTop5ByCityMapper.class);
        job.setReducerClass(TotalPriceTop5ByCityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
