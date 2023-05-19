package cn.lbj.house.partitioner;

import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner  extends Partitioner<SecondarySortWritable, NullWritable> {

    @Override
    public int getPartition(SecondarySortWritable key, NullWritable value, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
