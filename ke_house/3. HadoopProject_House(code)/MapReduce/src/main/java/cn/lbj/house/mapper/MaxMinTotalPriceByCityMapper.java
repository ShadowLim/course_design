package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxMinTotalPriceByCityMapper extends Mapper<Object, Text, Text, IntWritable> {

    // TODO
    //  fields: id city title houseInfo followInfo positionInfo total unitPrice tag crawler_time
    // TODO SQL:
    //  select city, max(total) from tb_house group by city;
    //  select city, min(total) from tb_house group by city;

    private Text outk = new Text();
    private IntWritable outv = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context out) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split("\t");

        outk.set(data[1]);      // city
        outv.set(Integer.parseInt(data[6]));        // total

//        System.out.println(data[0] + "-" + data[1] + "-" + data[6]);

        out.write(outk, outv);
    }
}
