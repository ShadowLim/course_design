package cn.lbj.house.bean;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HouseCntByPositionTopListBean implements Writable {

    private Text info;
    private IntWritable cnt;


    public Text getInfo() {
        return info;
    }

    public void setInfo(Text info) {
        this.info = info;
    }

    public IntWritable getCnt() {
        return cnt;
    }

    public void setCnt(IntWritable cnt) {
        this.cnt = cnt;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.cnt = new IntWritable(in.readInt());
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(cnt.get());
    }


    @Override
    public String toString() {
        String infoStr = info.toString();
        int idx = infoStr.indexOf("-");
        String city = infoStr.substring(0, idx);
        String position = infoStr.substring(idx + 1);

        return city + "#" + "[" + position + "]" + "#" + cnt;
    }
}
