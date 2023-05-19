package cn.whybigdata.mixuanMall.preprocess.service;

import cn.whybigdata.mixuanMall.preprocess.utils.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author：whybigdata
 * @date：Created in 20:48 2023-05-08
 * @description：
 */
public class _2_updateSales2And24 {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";
    private static String newSql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {

        List<Object[]> salesList = getNewSales();

        updateSales(salesList);

    }


    public static void updateSales(List<Object[]> newSalesList) {
        try {
            conn = dbUtil.getConnection();
            int size = newSalesList.size();

            for (Object[] newSalesArr : newSalesList) {
                String newSales2 = (String) newSalesArr[0];
                String newSales24 = (String) newSalesArr[1];
                Object id = newSalesArr[2];
                if (newSales2 != null) {
                    sql = "update tb_mx_mall set sales2 = " + "'" + newSales2 + "'" + " where id = " + id;
                    System.out.println("sql语句：" + sql);

                    ps = conn.prepareStatement(sql);
                    ps.executeUpdate();
                    System.out.println("第" + id + "条数据" + "更新成功！");
                    System.out.println("******************************************");
                }

                if (newSales24 != null) {
                    sql = "update tb_mx_mall set sales24 = " + "'" + newSales24 + "'" + " where id = " + id;
                    System.out.println("sql语句：" + sql);
                    ps = conn.prepareStatement(sql);
                    ps.executeUpdate();
                }

                System.out.println("第" + id + "条数据" + "更新成功！");

            }

            System.out.println("全部数据更新成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static List<Object[]> getNewSales() throws SQLException {
        List<Object[]> salesList = new ArrayList<Object[]>();
        int size = 0;

        try {
            conn = dbUtil.getConnection();
//            sql = "SELECT sales2, sales24, id FROM tb_mx_mall limit 10";
            sql = "SELECT sales2, sales24, id FROM tb_mx_mall";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                Object[] info = new Object[3];

                String oriSales2 = rs.getString(1);
                String oriSales24 = rs.getString(2);
                int id = rs.getInt(3);

                System.out.println("旧的sales2，sales24: " + oriSales2 + ", " + oriSales24);
                System.out.println("id:" + id);

//                if (oriSales2 == null) {
//                    // 对于【近2小时销售量为空值】这种情况，默认赋值为0，防止出现空指针异常
//                    oriSales2 = String.valueOf(0);
//                }
                if (oriSales2 != null) {
                    if (oriSales2.contains("万")) {
                        int index = oriSales2.indexOf("万");
                        oriSales2 = String.valueOf(Double.parseDouble(oriSales2.substring(0, index)) * 10000);
                    }
                    if (oriSales2.contains(".")) {
                        int index = oriSales2.indexOf(".");
                        oriSales2 = oriSales2.substring(0, index);
                    }
                }

//                if (oriSales24 == null) {
//                    // 对于【近24小时销售量为空值】这种情况，默认赋值为0，防止出现空指针异常
//                    oriSales24 = String.valueOf(0);
//                }
                if (oriSales24 != null) {
                    if (oriSales24.contains("万")) {
                        int index = oriSales24.indexOf("万");
                        oriSales24 = String.valueOf(Double.parseDouble(oriSales24.substring(0, index)) * 10000);
                    }
                    if (oriSales24.contains(".")) {
                        int index = oriSales24.indexOf(".");
                        oriSales24 = oriSales24.substring(0, index);
                    }
                }
                if (oriSales2 != null || oriSales24 != null) {
                    System.out.println("新的sales2，sales24:" + oriSales2 + ", " + oriSales24);
                    info[0] = oriSales2;
                    info[1] = oriSales24;
                    info[2] = id;

                    salesList.add(info);
                }

//                info[0] = oriSales2;
//                info[1] = oriSales24;
//                info[2] = id;
//
//                salesList.add(info);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("------------------------------------");
        for (Object[] infoStr : salesList) {
            System.out.println("(" + infoStr[0] + ", " + infoStr[1] + ")-->" + infoStr[1]);
        }

        return salesList;
    }
}
