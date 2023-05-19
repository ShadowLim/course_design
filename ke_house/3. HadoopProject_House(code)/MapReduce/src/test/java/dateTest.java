import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class dateTest {
    public static void main(String args[]) throws ParseException {
        Timestamp ts = Timestamp.valueOf("2022-10-24 12:26:00");
        System.out.println(ts);


        System.out.println("======================");


        String ct = "2022-10-24";
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date date = df.parse(ct);
        long time = date.getTime();

//        Timestamp ts1 = new Timestamp(time);

        String format = df.format(new Date(time - 2 * 24 * 60 * 60 * 1000));

        System.out.println(format);     // TODO 2022-10-22


        System.out.println("-------------------------------------");


        int size = 2;


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        Date getDate = sdf.parse(ct);
        String getTime = sdf.format(getDate);
        System.out.println(getTime);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(getDate);
//        calendar.add(Calendar.DAY_OF_WEEK,-0);
//        calendar.add(Calendar.DAY_OF_WEEK,-2);
//        calendar.add(Calendar.DAY_OF_WEEK,-size);
        calendar.add(Calendar.MONTH, -size);
        Date pubDate = calendar.getTime();

        String pubTime = sdf.format(pubDate);

        System.out.println("开始时间: " + getTime);
        System.out.println("结束时间: " + pubTime);

    }
}
