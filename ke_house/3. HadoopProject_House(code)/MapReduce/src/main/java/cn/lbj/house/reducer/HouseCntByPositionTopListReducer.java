package cn.lbj.house.reducer;

import cn.lbj.house.bean.HouseCntByPositionTopListBean;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HouseCntByPositionTopListReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private NullWritable outv = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        StringBuffer city_position_cnt = new StringBuffer();
        HouseCntByPositionTopListBean topListBean = new HouseCntByPositionTopListBean();

        for (IntWritable value : values) {
            sum += value.get();
        }

        topListBean.setCnt(new IntWritable(sum));
        topListBean.setInfo(key);

        key.set(topListBean.toString());

        context.write(key, outv);
    }
}
