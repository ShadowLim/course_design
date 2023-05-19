package cn.lbj.house.service;

import cn.lbj.house.util.DBUtil;

import java.sql.*;

public class NullTitleProcess {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static ResultSet rs = null;
    private static String sql = "";

    private static PreparedStatement ps = null;

    public static void main(String[] args) {

        try {
            conn = dbUtil.getConnection();
            sql = "delete from tb_house where title = '' ";
            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            ps.executeUpdate();

            System.out.println("空数据删除成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
