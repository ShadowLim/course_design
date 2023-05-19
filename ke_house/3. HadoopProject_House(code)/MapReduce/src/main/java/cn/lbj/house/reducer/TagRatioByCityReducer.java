package cn.lbj.house.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class TagRatioByCityReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text outv = new Text();
    private int sum = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        DecimalFormat df = new DecimalFormat("0.00");
        int cnt = 0;

        for (IntWritable value : values) {
            cnt += value.get();
        }

        String s = key.toString();
        String format = "";
//        System.out.println(s + "==============");
        if (s.contains("上海")) {
            sum = 2995;
            format = df.format((double) cnt / sum * 100) + "%";
        } else if (s.contains("北京")) {
            sum = 2972;
            format = df.format((double) cnt / sum * 100) + "%";
        } else if (s.contains("广州")) {
            sum = 2699;
            format = df.format((double) cnt / sum * 100) + "%";
        } else {
            sum = 2982;
            format = df.format((double) cnt / sum * 100) + "%";
        }
        outv.set(format);

        context.write(key, outv);
    }
}
