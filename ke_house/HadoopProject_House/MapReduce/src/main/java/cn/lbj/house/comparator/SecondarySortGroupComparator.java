package cn.lbj.house.comparator;

import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupComparator  extends WritableComparator {
    protected SecondarySortGroupComparator() {
        super(SecondarySortWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        SecondarySortWritable ip1 = (SecondarySortWritable) w1;
        SecondarySortWritable ip2 = (SecondarySortWritable) w2;
        return SecondarySortWritable.compare(ip1.getFirst(), ip2.getFirst());
    }
}
