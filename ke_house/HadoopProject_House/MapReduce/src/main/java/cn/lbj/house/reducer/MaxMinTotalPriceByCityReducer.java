package cn.lbj.house.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MaxMinTotalPriceByCityReducer extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        List<Integer> totalList = new ArrayList<Integer>();

        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            totalList.add(iterator.next().get());
        }

        Collections.sort(totalList);
        int max = totalList.get(totalList.size() - 1);
        int min = totalList.get(0);

        Text outv = new Text();
        outv.set("房子总价最大、小值分别为：" + String.valueOf(max) + "万元," + String.valueOf(min) + "万元");

        context.write(key, outv);

    }
}
