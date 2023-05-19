package cn.lbj.house.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;

public class AcessHousePubTimeSortMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String lines = value.toString();
        String data[] = lines.split("\t");

     z
        String crawler_time = data[9];
//        System.out.println("时间字段为:" + crawler_time);

        // TODO 2022-10-24
        String ct = crawler_time.substring(0, 10);
//        System.out.println("获取字段crawler_time：" + ct);

//        // TODO ISO格式： 直接将 String 解析为返回本地日期的 parse() 方法。
//        LocalDate parse_ct = LocalDate.parse(ct);

        // TODO 2人关注|3天前发布
        //  2人关注|今天发布
        //  2人关注|1月前发布
        String followInfo = data[4];
        int idx1 = followInfo.indexOf("|");
        int idx2 = followInfo.indexOf("发");
        String timeStr = followInfo.substring(idx1 + 1, idx2);

        String pubDate = "";
        try {
            pubDate = getPubDate(ct, timeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        outk.set(new Text(pubDate));


        context.write(outk, outv);
    }


    public String getPubDate(String ct, String timeStr) throws ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date getTime = sdf.parse(ct);
        String getDate = sdf.format(getTime);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(getTime);

        if (timeStr.equals("今天")) {
            calendar.add(Calendar.DAY_OF_WEEK,-0);
        } else if (timeStr.contains("天")) {
            int i = 0;
            while (Character.isDigit(timeStr.charAt(i))) {
                i++;
            }
            int size = Integer.parseInt(timeStr.substring(0, i));
//            System.out.println("减去的大小：" + size);
            calendar.add(Calendar.DAY_OF_WEEK, -size);
        } else {
            int i = 0;
            while (Character.isDigit(timeStr.charAt(i))) {
                i++;
            }
            int size = Integer.parseInt(timeStr.substring(0, i));
            calendar.add(Calendar.MONTH, -size);
        }

        Date pubTime = calendar.getTime();
        String pubDate = sdf.format(pubTime);

//        System.out.println("获取到的日期: " + getDate);
//        System.out.println("实际发布日期: " + pubDate);

        return pubDate;
    }

}
