package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TagRatioByCityMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\t");
        String city = data[1];
        String tag = data[8];
        if ("".equals(tag)) {
            tag = "未知标签";
        }
        //        System.out.println(city + "-" + tag + "-");
        outk.set(city + "-" + tag);
        context.write(outk, outv);
    }
}
