package cn.lbj.house.service;

import cn.lbj.house.util.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UpdateTitleWithTab {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {

//        getRealTitle();

        List<Object[]> realTitle = getRealTitle();

        updateTitle(realTitle);

    }


    public static void updateTitle(List<Object[]> newTitleList) {
        try {
            conn = dbUtil.getConnection();
            for (Object[] newTitleArr : newTitleList) {
                String newTitle = "";
                Object id = 0;
                for (int i = 0; i < newTitleArr.length; i++) {
                    newTitle = (String) newTitleArr[0];
                    id = newTitleArr[1];
//                    id = Integer.parseInt((String) newTitleArr[1]);
                }
                System.out.println("新的字段信息为：" + newTitle);
                sql = "update tb_house set title = " + "'" + newTitle + "'" + " where id = " + id;
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



    public static List<Object[]> getRealTitle() throws SQLException {
        List<Object[]> titleList = new ArrayList<Object[]>();
        int size = 0;

        try {
            conn = dbUtil.getConnection();
            sql = "SELECT title, id FROM tb_house where title like '%\\t%'";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Object[] info = new Object[2];
                
                String title = rs.getString(1);
                int id = rs.getInt(2);
                
                System.out.println("旧的title:" + title);
                System.out.println("id:" + id);

                String newTitle = title.replace("\t", ",");
                System.out.println("新的title:" + newTitle);

                info[0] = newTitle;
                info[1] = id;

                titleList.add(info);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("------------------------------------");
        for (Object[] infoStr : titleList) {
            System.out.println(infoStr[0] + "-->" + infoStr[1]);
        }

        return titleList;
    }
}
