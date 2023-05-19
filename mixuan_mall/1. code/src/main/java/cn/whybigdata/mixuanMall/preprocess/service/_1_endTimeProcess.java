package cn.whybigdata.mixuanMall.preprocess.service;

import cn.whybigdata.mixuanMall.preprocess.utils.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author：whybigdata
 * @date：Created in 20:39 2023-05-08
 * @description：
 */
public class _1_endTimeProcess {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";
    private static String newSql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {

        List<Object[]> ednTimeList = getOriEndTime();

        updateEndTime(ednTimeList);

    }


    public static void updateEndTime(List<Object[]> newSoldList) {
        try {
            conn = dbUtil.getConnection();

            for (Object[] newSoldArr : newSoldList) {
                String newEndTime = "";
                Object id = 0;
                for (int i = 0; i < newSoldArr.length; i++) {
                    newEndTime = (String) newSoldArr[0];
                    if (newEndTime.contains("万")) {
                        int index = newEndTime.indexOf("：");
                        newEndTime = newEndTime.substring(index + 1);
                    }
                    id = newSoldArr[1];
//                    id = Integer.parseInt((String) newSoldArr[1]);
                }
                System.out.println("新的endTime字段信息为：" + newEndTime);
                sql = "update tb_mx_mall set endTime = " + "'" + newEndTime + "'" + " where id = " + id;
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


    public static List<Object[]> getOriEndTime() throws SQLException {
        List<Object[]> ednTimeList = new ArrayList<Object[]>();
        int size = 0;

        try {
            conn = dbUtil.getConnection();
//            sql = "SELECT endTime, id FROM tb_mx_mall limit 2";
            sql = "SELECT endTime, id FROM tb_mx_mall";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Object[] info = new Object[2];

                String oriEndTime = rs.getString(1);
                int id = rs.getInt(2);

                System.out.println("旧的endTime:" + oriEndTime);
                System.out.println("id:" + id);

                String newEndTime = oriEndTime.replace("结束：", "");
                System.out.println("新的ndTime:" + newEndTime);

                info[0] = newEndTime;
                info[1] = id;

                ednTimeList.add(info);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("------------------------------------");
        for (Object[] infoStr : ednTimeList) {
            System.out.println(infoStr[0] + "-->" + infoStr[1]);
        }

        return ednTimeList;
    }
}
