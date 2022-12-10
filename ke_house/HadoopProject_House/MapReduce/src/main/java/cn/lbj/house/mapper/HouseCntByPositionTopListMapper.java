package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HouseCntByPositionTopListMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text info = new Text();
    private IntWritable cnt = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\t");
        String cityAndPosition = data[1] + "-" + data[5];
        info.set(cityAndPosition);
        context.write(info, cnt);
    }
}
