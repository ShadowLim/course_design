import java.text.DecimalFormat;

public class compute {

    public static void main(String[] args) {

        DecimalFormat df = new DecimalFormat("0.00");//格式化小数，不足的补0

        int a=100;
        int b=9;

        String num = df.format((float) a / b);
        System.out.println(num);    // 11.11

    }
}
