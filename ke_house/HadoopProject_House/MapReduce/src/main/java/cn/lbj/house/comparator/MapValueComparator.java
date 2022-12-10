package cn.lbj.house.comparator;

import org.apache.hadoop.io.Text;

import java.util.Comparator;
import java.util.Map;

public class MapValueComparator implements Comparator<Map.Entry<Text, Integer>> {

    @Override
    public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
        return o2.getValue().compareTo(o1.getValue());
    }
}
