package com.lbj.service;


import com.lbj.bean.JobUtil;

import java.math.RoundingMode;
import java.sql.*;
import java.text.NumberFormat;


/**
 * @description：数据预处理阶段
 * @date：Created in 21:57 2022/6/7
 */
public class DataDeal {

    private static JobUtil jobUtil = new JobUtil();

    private static String sql = "";
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet resultSet = null;

    private static String newSQL = "";
    private static Connection innerConn = null;
    private static PreparedStatement ps = null;

    private static String status = "";

    public static void main(String[] args) {

//        posDeal();

//        salaryPre();

//        deleteNull();

//        statusDeal();

//        positionDeal();

//        salaryPreDeal0();

//        salaryPreDeal1();

//        salaryDeal();

//        salaryCalculate();

//        rateDeal();

//        nullDurationDeal();

//        durationDeal();


    }



    /**
     * 反馈时长的处理
     *  -- 将 ”7天“ 转换为 ”7“
     */
    public static void durationDeal() {
        String newDuration = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, duration from job where id > 2366";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String duration = resultSet.getString(2);

                System.out.println(id);
                System.out.println(duration);

                int idx = duration.indexOf("天");
                newDuration = duration.substring(0, idx);
                System.out.println("newDuartion：" + newDuration);

                newSQL = "update job set duration = '" + newDuration + "' where id = " + id;
                System.out.println(newSQL);

                try {
                    innerConn = jobUtil.getConn();
                    ps = innerConn.prepareStatement(newSQL);
                    int row = ps.executeUpdate();
                    System.out.println("改变了" + row + "行");

                    ps.close();
                    innerConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 空反馈时长的处理
     *  -- 空反馈时长    --> 自定义为 7天
     */
    public static void nullDurationDeal() {
        String newDuration = "7天";

        try {
            conn = jobUtil.getConn();
            sql = "select id, duration from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String duration = resultSet.getString(2);

                System.out.println(id);
                System.out.println(duration);

                if (duration == null) {
                    newSQL = "update job set duration = '" + newDuration + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 空反馈率处理
     *  -- 反馈率为空    --> 自定义设置为 0.5
     */
    public static void rateDeal() {
        String newRate = "0.5";

        try {
            conn = jobUtil.getConn();
            sql = "select id, rate from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String rate = resultSet.getString(2);

                System.out.println(id);
                System.out.println(rate);

                if (rate == null) {
                    newSQL = "update job set rate = '" + newRate + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 薪资计算
     *  -- 非实习：以万元/月为单位
     *  -- 实习：  以千元/月为单位(每月固定为30天)
     */
    public static void salaryCalculate() {
        String newSalary = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position, salary from job where id >= 1062";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String salary = resultSet.getString(3);

                System.out.println(id);
                System.out.println(pos);
                System.out.println(salary);

                if (salary.contains("薪") || salary.contains("元")) {
                    // 非实习：以万元/月为单位
                    if (pos.contains("_1")) {
                        // 9K*12薪
                        int idx1 = salary.indexOf("K");
                        int idx2 = salary.indexOf("*");

                        int num1 = Integer.parseInt(salary.substring(0, idx1));
                        int num2 = Integer.parseInt(salary.substring(idx2 + 1, salary.length() - 1));

                        System.out.println(num1);
                        System.out.println(num2);

                        double sum = 0.0;
                        double temp = (double) num1 / 10;
                        System.out.println(temp);

                        sum += (temp * num2) / 12;

                        NumberFormat nf = NumberFormat.getCurrencyInstance();
                        nf.setMaximumFractionDigits(1);         // 保留一位小数
                        nf.setRoundingMode(RoundingMode.UP);    // 四舍五入

                        newSalary = nf.format(sum).substring(1);
                        System.out.println(newSalary);

                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    } else if (pos.contains("_0")) {   // 实习：  以千元/月为单位(每月固定为30天)
                        // 350元/天
                        int idx = salary.indexOf("元");

                        int num = Integer.parseInt(salary.substring(0, idx));

                        System.out.println(num);

                        double sum = (double)num * 30 / 1000;

                        NumberFormat nf = NumberFormat.getCurrencyInstance();
                        nf.setMaximumFractionDigits(1);         // 保留一位小数
                        nf.setRoundingMode(RoundingMode.UP);    // 四舍五入

                        newSalary = nf.format(sum).substring(1);
                        System.out.println(newSalary);

                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 薪资平均化处理：
     *  -- 为了方便统计 薪资取区间的中间值(默认下取整)
     *  -- 8-10K*12薪     -->  9K*12薪
     *  -- 15-30K*15薪    -->  22K*15薪
     *  -- 300-400元/天   -->  350元/天
     */
    public static void salaryDeal() {
        String newSalary = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position, salary from job where id >= 2160";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String salary = resultSet.getString(3);

                System.out.println(id);
                System.out.println(pos);
                System.out.println(salary);

                if (salary.contains("-")) {
                    if (pos.contains("_1")) {
                        // 8-10K*12薪   -->  9K*12薪
                        // 15-30K*15薪   -->  22K*15薪
                        int idx1 = salary.indexOf("-");
                        int idx2 = salary.indexOf("K");
                        int idx3 = salary.indexOf("*");

                        int num1 = Integer.parseInt(salary.substring(0, idx1));
                        int num2 = Integer.parseInt(salary.substring(idx1 + 1, idx2));
//                    int num3 = Integer.parseInt(salary.substring(idx3 + 1, salary.length() - 1));

                        System.out.println(num1);
                        System.out.println(num2);
//                    System.out.println(num3);

                        int average = (num1 + num2) / 2;

                        newSalary = String.valueOf(average) + "K" + salary.substring(idx3);

                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    } else if (pos.contains("_0")) {
                        // 300-400元/天   -->  350元/天
                        int idx1 = salary.indexOf("-");
                        int idx2 = salary.indexOf("元");

                        int num1 = Integer.parseInt(salary.substring(0, idx1));
                        int num2 = Integer.parseInt(salary.substring(idx1 + 1, idx2));

                        System.out.println(num1);
                        System.out.println(num2);

                        int average = (num1 + num2) / 2;

                        newSalary = String.valueOf(average) + salary.substring(idx2);

                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 预处理 非实习岗位的"薪资面议"    薪资面议 --> 13K*12薪
     */
    public static void salaryPreDeal1() {
        String newSalary = "13K*12薪";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position, salary from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String salary = resultSet.getString(3);

                System.out.println(id);
                System.out.println(pos);
                System.out.println(salary);

                String diff = pos.substring(pos.length() - 2);

                System.out.println(diff);

                if ("薪资面议".equals(salary)) {
                    if ("_1".equals(diff)) {
                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 预处理 实习岗位的"薪资面议"    薪资面议 --> 150元/天
     */
    public static void salaryPreDeal0() {
        String newSalary = "150元/天";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position, salary from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String salary = resultSet.getString(3);

                System.out.println(id);
                System.out.println(pos);
                System.out.println(salary);

                String diff = pos.substring(pos.length() - 2);

                System.out.println(diff);

                if ("薪资面议".equals(salary)) {
                    if ("_0".equals(diff)) {
                        newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                        System.out.println(newSQL);

                        try {
                            innerConn = jobUtil.getConn();
                            ps = innerConn.prepareStatement(newSQL);
                            int row = ps.executeUpdate();
                            System.out.println("改变了" + row + "行");

                            ps.close();
                            innerConn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 岗位处理
     */
    public static void positionDeal() {
        String newPos = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position from job where id > 200";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);

                System.out.println(id);
                System.out.println(pos);

                if (pos.contains("数据")) {
                    newPos = "数据岗" + pos.substring(pos.length() - 2);

                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                } else if (pos.contains("运营")) {
                    newPos = "运营岗"+ pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("算法")) {
                    newPos = "算法岗" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("开发") || pos.contains("后端") || pos.contains("前端")
                        || pos.contains("测试") || pos.contains("客户")  || pos.contains("Java")
                        || pos.contains("C++")) {
                    newPos = "开发岗" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("工程")) {
                    newPos = "工程师" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("设计")) {
                    newPos = "设计岗" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("策划")) {
                    newPos = "策划岗" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else if (pos.contains("培训")) {
                    newPos = "培训岗" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {
                    newPos = "其他" + pos.substring(pos.length() - 2);
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");
                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            resultSet.close();
            statement.close();
            conn.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


    /**
     * 区分实习与非实习
     */
    public static void statusDeal() {
        String newPos = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);

                System.out.println(id);
                System.out.println(pos);

                // 实习岗位
                if (pos.contains("实-") || pos.contains("实习") || pos.contains("转正")) {
                    newPos = pos + "_0";
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                } else {    // 非实习岗位
                    newPos = pos + "_1";
                    newSQL = "update job set position =  '" +  newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    /**
     * 删除空行的数据
     */
    public static void deleteNull() {
        String newPos = "";

        try {
            conn = jobUtil.getConn();
            sql = "select * from job";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String address = resultSet.getString(3);
                String education = resultSet.getString(4);
                String salary = resultSet.getString(5);
                String company = resultSet.getString(6);
                String rate = resultSet.getString(7);
                String duration = resultSet.getString(8);


                if (pos == null && address == null && education == null &&
                    salary == null && company == null &&  rate == null &&
                    duration == null) {
                    newSQL = "delete from job" + " where id = " + id;
                    System.out.println(id);
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }




    public static void pre() {
        String newPos = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position from job where id between 1 and 670";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);

                System.out.println(id);
                System.out.println(pos);


                int idx = pos.indexOf("_");
                newPos = pos.substring(0, idx);
                newSQL = "update job set position = '" + newPos + "' where id = " + id;
                System.out.println(newSQL);

                try {
                    innerConn = jobUtil.getConn();
                    ps = innerConn.prepareStatement(newSQL);
                    int row = ps.executeUpdate();
                    System.out.println("改变了" + row + "行");

                    ps.close();
                    innerConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public static void salaryPre() {
        String newSalary = "薪资面议";

        try {
            conn = jobUtil.getConn();
            sql = "select id, salary from job where id";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String salary = resultSet.getString(2);

                System.out.println(id);
                System.out.println(salary);

                if ("13K*12薪".equals(salary)) {
                    newSQL = "update job set salary = '" + newSalary + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public static void posDeal() {
        String newPos = "";

        try {
            conn = jobUtil.getConn();
            sql = "select id, position, salary from job where id >= 1062";
            statement = conn.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                String pos = resultSet.getString(2);
                String salary = resultSet.getString(3);

                System.out.println(id);
                System.out.println(pos);
                System.out.println(salary);

                if (salary.contains("元") && pos.contains("1")) {
                    int idx = pos.indexOf("_");
                    newPos = pos.substring(0, idx + 1) + "0";
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                if (salary.contains("薪") && pos.contains("0")) {
                    int idx = pos.indexOf("_");
                    newPos = pos.substring(0, idx + 1) + "1";
                    newSQL = "update job set position = '" + newPos + "' where id = " + id;
                    System.out.println(newSQL);

                    try {
                        innerConn = jobUtil.getConn();
                        ps = innerConn.prepareStatement(newSQL);
                        int row = ps.executeUpdate();
                        System.out.println("改变了" + row + "行");

                        ps.close();
                        innerConn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
