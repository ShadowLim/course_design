import cn.whybigdata.mixuanMall.preprocess.utils.DBUtil;

import java.sql.*;

/**
 * @author：whybigdata
 * @date：Created in 22:01 2023-05-07
 * @description：
 */
public class GetRowById {
    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {

        try {
            conn = dbUtil.getConnection();
            for (int i = 1; i <= 15315; i++) {
                sql = "select price from tb_mx_mall " + " where id = " + i;
//                System.out.println("sql语句：" + sql);
                ps = conn.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
                while (!resultSet.next()) {
                    System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + i);
                    break;
                }
            }
            System.out.println("全部数据查询完毕！");
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

}
