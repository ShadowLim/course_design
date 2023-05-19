package cn.lbj.house.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TotalPriceTop5ByCityReducer extends Reducer<Text, IntWritable, Text, Text> {

   private Text outv = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        List<Integer> totalPriceList = new ArrayList<Integer>();

        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            totalPriceList.add(iterator.next().get());
        }

        Collections.sort(totalPriceList);
        int size = totalPriceList.size();
        String top5Str = "二手房总价Top5：";
        for (int i = 1; i <= 5; i++) {
            if (i == 5) {
                top5Str += totalPriceList.get(size - i) + "万元";
            } else {
                top5Str += totalPriceList.get(size - i) + "万元, ";
            }
        }

        outv.set(String.valueOf(top5Str));

        context.write(key, outv);
    }

//        @Override
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        Map<Text, Integer> totalPriceMap = new TreeMap<>();
//        Iterator<IntWritable> iterator = values.iterator();
//        while (iterator.hasNext()) {
////            System.out.println("--- " + iterator.next().get() + " --------");
//            len++;
//            totalPriceMap.put(key, iterator.next().get());
//        }
//        System.out.println("---- " + len + "========");
//
//
//        for (Integer value : totalPriceMap.values()) {
//            System.out.println(value);
//            System.out.println("===========");
//        }
//
//        List<Map.Entry<Text, Integer>> entryList = new ArrayList<Map.Entry<Text, Integer>>(totalPriceMap.entrySet());
//        Collections.sort(entryList, new MapValueComparator());
//
//        Map<Text, Integer> sortedMap = new LinkedHashMap<Text, Integer>();
//        Iterator<Map.Entry<Text, Integer>> iter = entryList.iterator();
//        Map.Entry<Text, Integer> tmpEntry = null;
//        while (iter.hasNext()) {
//            tmpEntry = iter.next();
//            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
//        }
//
//        for (int i = 0; i < sortedMap.values().size(); i++) {
//            System.out.println(sortedMap.values());
//            System.out.println("------------");
//        }
//
//        for (Integer value : sortedMap.values()) {
//            outv.set(String.valueOf(value) + "万元");
//            context.write(key, outv);
//        }
//
//    }
}

