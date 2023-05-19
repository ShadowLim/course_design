package cn.lbj.house.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AcessHousePubTimeSortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
//            System.out.println("<" + key + "," + val + ">");
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));

    }
}
