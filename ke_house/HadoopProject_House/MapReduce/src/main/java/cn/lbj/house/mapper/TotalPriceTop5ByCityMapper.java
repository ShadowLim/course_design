package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TotalPriceTop5ByCityMapper extends Mapper<Object, Text, Text, IntWritable> {
    private int cnt = 1;
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] data = line.split("\t");
        String city = data[1];
        String totalPrice = data[6];

//        System.out.println(data[1] + "--> " + data[6] + " --> 第" + cnt + "条");
//        System.out.println("----------------------------");
//        cnt++;

        outk.set(data[1]);
        outv.set(Integer.parseInt(data[6]));

        context.write(outk, outv);
    }
}
