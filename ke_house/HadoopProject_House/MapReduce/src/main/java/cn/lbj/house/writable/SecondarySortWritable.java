package cn.lbj.house.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondarySortWritable implements WritableComparable<SecondarySortWritable> {

    private int first;
    private int second;

    public SecondarySortWritable() {
    }

    public SecondarySortWritable(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SecondarySortWritable) {
            SecondarySortWritable ip = (SecondarySortWritable) o;
            return first == ip.first && second == ip.second;
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    public int compareTo(SecondarySortWritable ip) {
        int cmp = compare(first, ip.first);
        if (cmp != 0) {
            return cmp;
        }
        return compare(second, ip.second);
    }

    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

}