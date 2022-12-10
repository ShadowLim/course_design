import cn.lbj.house.mapper.HouseCntByCityMapper;
import cn.lbj.house.partitioner.CityPartitioner;
import cn.lbj.house.reducer.HouseCntByCityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HouseCntByCityDriver {
    public static void main(String[] args) throws Exception {
        args = new String[] { "/input/datas/tb_house.txt", "/output/HouseCntByCity" };
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://lbj-VirtualBox:9000");
        Job job = Job.getInstance(conf, "HouseCntByCity");
        job.setJarByClass(cn.lbj.house.driver.HouseCntByCityDriver.class);
        job.setMapperClass(HouseCntByCityMapper.class);
        job.setReducerClass(HouseCntByCityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setPartitionerClass(CityPartitioner.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}