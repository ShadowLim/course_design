package com.lbj.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @description：工具类
 * @date：Created in 19:20 2022/6/10
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



}
