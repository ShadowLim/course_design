package com.lbj.bean;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * @date：Created in 22:02 2022/6/7
 */
public class JobUtil {
    private static String driver = null;
    private static String url = null;
    private static String user = null;
    private static String pwd = null;
    Connection conn = null;
    ResultSet rs = null;

    /**
     * 获取数据库连接
     * @return
     */
    public Connection getConn() {
        try {
            Properties pro = new Properties();  // 用于获取 dbConfig.properties 的参数
            pro.load(this.getClass().getClassLoader().getResourceAsStream("jdbc.properties"));
            String driver = pro.getProperty("driver");
            String url = pro.getProperty("url");
            String user = pro.getProperty("user");
            String pwd = pro.getProperty("pwd");
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pwd);
            return conn  ;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

//    public int executeUpdate(String sql) {
//        int result = 0;
//        try {
//            Properties pro = new Properties();  // 用于获取 dbConfig.properties 的参数
//            pro.load(this.getClass().getClassLoader().getResourceAsStream("jdbc.properties"));
//            String driver = pro.getProperty("driver");
//            String url = pro.getProperty("url");
//            String user = pro.getProperty("user");
//            String pwd = pro.getProperty("pwd");
//            Class.forName(driver);
//            conn = DriverManager.getConnection(url, user, pwd);
//            Statement stmt = conn.createStatement();
//            result = stmt.executeUpdate(sql);
//        } catch(SQLException | IOException | ClassNotFoundException ex) {
//            System.out.println("执行sql语句，数据库连接异常！！！");
//            ex.printStackTrace();
//        }
//        return result;
//    }
}
