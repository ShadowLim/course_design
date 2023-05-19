package cn.lbj.house.comparator;

import cn.lbj.house.writable.SecondarySortWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator  extends WritableComparator {
    protected SecondarySortComparator() {
        super(SecondarySortWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        SecondarySortWritable ip1 = (SecondarySortWritable) w1;
        SecondarySortWritable ip2 = (SecondarySortWritable) w2;
        int cmp = SecondarySortWritable.compare(ip1.getFirst(), ip2.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return -SecondarySortWritable.compare(ip1.getSecond(), ip2.getSecond());
    }
}
