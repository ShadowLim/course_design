package cn.whybigdata.mixuanMall.preprocess.service;

import cn.whybigdata.mixuanMall.preprocess.utils.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author：whybigdata
 * @date：Created in 16:21 2023-05-08
 * @description：
 */
public class _0_updateSoldProcess {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";
    private static String newSql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {

        List<Object[]> soldList = getOriSold();

        updateSold(soldList);

    }


    public static void updateSold(List<Object[]> newSoldList) {
        try {
            conn = dbUtil.getConnection();

            for (Object[] newSoldArr : newSoldList) {
                String newSold = "";
                Object id = 0;
                for (int i = 0; i < newSoldArr.length; i++) {
                    newSold = (String) newSoldArr[0];
                    if (newSold.contains("万")) {
                        int index1 = newSold.indexOf(".");
                        int index2 = newSold.indexOf("万");
//                        int before = Integer.parseInt(newSold.substring(0, index1)) * 10000;
//                        int after = Integer.parseInt(newSold.substring(index1 + 1, index2)) * 1000;
//                        System.out.println(before + after);
//                        newSold = String.valueOf(before + after);
                        newSold = String.valueOf(Double.parseDouble(newSold.substring(0, index2)) * 10000);
                    }
                    if (newSold.contains(".")) {
                        int index = newSold.indexOf(".");
                        newSold = newSold.substring(0, index);
                    }
                    id = newSoldArr[1];
//                    id = Integer.parseInt((String) newSoldArr[1]);
                }
                System.out.println("新的Sold字段信息为：" + newSold);
                sql = "update tb_mx_mall set sold = " + "'" + newSold + "'" + " where id = " + id;
                System.out.println("sql语句：" + sql);

                ps = conn.prepareStatement(sql);
                ps.executeUpdate();
                System.out.println("第" + id + "条数据" + "更新成功！");

            }
            System.out.println("全部数据更新成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static List<Object[]> getOriSold() throws SQLException {
        List<Object[]> soldList = new ArrayList<Object[]>();
        int size = 0;

        try {
            conn = dbUtil.getConnection();
            sql = "SELECT sold, id FROM tb_mx_mall";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Object[] info = new Object[2];

                String oriSold = rs.getString(1);
                int id = rs.getInt(2);

                System.out.println("旧的sold:" + oriSold);
                System.out.println("id:" + id);

                String newSold = oriSold.replace("已售：", "");
                System.out.println("新的sold:" + newSold);

                info[0] = newSold;
                info[1] = id;

                soldList.add(info);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("------------------------------------");
        for (Object[] infoStr : soldList) {
            System.out.println(infoStr[0] + "-->" + infoStr[1]);
        }

        return soldList;
    }
}
