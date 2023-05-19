package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HouseCntByCityMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\t");

        outk.set(new Text(data[1]));

        System.out.println("-----------------------------------");

        System.out.println(data[0] + "-->" + data[1]);

        context.write(outk, outv);

    }
}
