package cn.lbj.house.driver;

import cn.lbj.house.comparator.SecondarySortComparator;
import cn.lbj.house.comparator.SecondarySortGroupComparator;
import cn.lbj.house.mapper.SecondarySortMapper;
import cn.lbj.house.partitioner.SecondarySortPartitioner;
import cn.lbj.house.reducer.SecondarySortReducer;
import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondarySortDriver {
    public static void main(String[] args) throws Exception {
        args = new String[] {"datas/tb_house.txt", "MapReduce/out/SecondarySort" };

        if (args.length != 2) {
            System.err.println("<input> <output>");
            System.exit(127);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(SecondarySortMapper.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setSortComparatorClass(SecondarySortComparator.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);

        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(SecondarySortWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
