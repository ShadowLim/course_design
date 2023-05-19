package cn.lbj.house.util;

import java.sql.*;
import java.util.Properties;

public class DBUtil {

    private static Properties properties = new Properties();  // 用于获取 dbConfig.properties 的参数
    private static String driver = null;
    private static String url = null;
    private static String user = null;
    private static String pwd = null;
    Connection conn = null;
    ResultSet rs = null;

    static {
        try {
            properties.load(DBUtil.class.getClassLoader().getResourceAsStream("db.properties"));
            driver = properties.getProperty("driver");
            Class.forName(driver);
            System.out.println("数据库连接成功！");
        } catch(Exception e) {
            e.printStackTrace();
            System.out.println("数据库连接失败");
        }
    }

    /**
     * TODO 获取数据库连接
     * @return
     */
    public static Connection getConnection(){
        Connection conn = null;
        try{
            String url = properties.getProperty("url");
            String user = properties.getProperty("user");
            String pwd = properties.getProperty("pwd");
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    public static void close(ResultSet rs, Statement st, Connection conn){
        try {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(getConnection());
    }

}
