package cn.lbj.house.mapper;

import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, SecondarySortWritable, NullWritable> {
    private SecondarySortWritable ssw = new SecondarySortWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");

//        String[] split = data[3].split("|");
//        String startYear = split[2];

        int idx1 = data[3].indexOf("|");
        int idx2 = data[3].indexOf("|", idx1 + 1);
        int idx4 = data[3].indexOf("建");
//        System.out.println(idx1 + "-" + idx2 + "-" + "-" + idx4);
//        System.out.println("--------" + data[3].substring(idx2 + 1, idx4 + 1) + "-------");
        String startYear = data[3].substring(idx2 + 1, idx4 + 1);
        String year = "";
        if (startYear.equals("未透露年份建")) {
            year = "9999";
        } else {
            year = startYear.substring(0, 4);
        }

        String total = data[6];

//        System.out.println(startYear + "-->" + year + "-->" + total);
        ssw = new SecondarySortWritable(Integer.parseInt(year), Integer.parseInt(total));
        context.write(ssw, NullWritable.get());

    }
}
