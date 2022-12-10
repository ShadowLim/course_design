package cn.lbj.house.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CityPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable intWritable, int i) {
        // TODO
        //  return (Integer.parseInt(key.toString())& Integer.MAX_VALUE)%numPartitions;

        if (key.toString().equals("北京")) {
            System.out.println("=================北京Partition0=============");
            return 0;
        } else if (key.toString().equals("上海")) {
            System.out.println("=================上海Partition1=============");
            return 1;
        } else if (key.toString().equals("广州")) {
            System.out.println("=================广州Partition2=============");
            return 2;
        } else {    // TODO 深圳
            System.out.println("=================深圳Partition3=============");
            return 3;
        }
    }

    public CityPartitioner() {
        super();
    }
}
