package cn.lbj.house.reducer;

import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<SecondarySortWritable, NullWritable, SecondarySortWritable, NullWritable> {

    @Override
    protected void reduce(SecondarySortWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable val : values) {
            context.write(key, NullWritable.get());
        }
    }
}
