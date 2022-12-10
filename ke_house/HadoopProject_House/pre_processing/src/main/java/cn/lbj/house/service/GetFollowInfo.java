package cn.lbj.house.service;

import cn.lbj.house.util.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GetFollowInfo {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) throws SQLException {
//        getRealFollowInfo();

        List<String[]> realFollowInfo = getRealFollowInfo();

        updateFollowInfo(realFollowInfo);
    }


    public static void updateFollowInfo(List<String[]> newHouseInfo) {

        try {
            conn = dbUtil.getConnection();
            int cnt = 1;
            for (String[] newInfoArr : newHouseInfo) {
                String newInfo = "";
                for (String subInfo : newInfoArr) {
                    newInfo += subInfo;
                }
                System.out.println("新的字段信息为：" + newInfo);
                sql = "update tb_house set followInfo = " + "'" + newInfo + "'" + " where id = " + cnt;
                System.out.println("sql语句：" + sql);
                ps = conn.prepareStatement(sql);
                ps.executeUpdate();
                System.out.println("第" + cnt + "条数据" + "更新成功！");
                cnt++;
            }
            System.out.println("全部数据更新成功！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    public static List<String[]> getRealFollowInfo() throws SQLException {
        List<String[]> infoList = new ArrayList<String[]>();
        int size = 0;

        try {
            conn = dbUtil.getConnection();
            sql = "select followInfo from tb_house";
//            sql = "select followInfo from tb_house limit 1";
//            sql = "select followInfo from tb_house limit 30, 100";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                String[] info = new String[2];
                String followStr = rs.getString(1);
//                System.out.println(followStr);
                String followReplaceStr = followStr.replace(" ", "");
                System.out.println("replace空格处理后的信息字段为：");
                System.out.println(followReplaceStr);
                System.out.print("replace空格处理后的信息字段长度为：");
                System.out.println(followReplaceStr.length());
                System.out.println("*******************************");

                /**
                 * TODO follwInfo字段信息：
                 *   0人关注 / 今天发布
                 *   1人关注 / 3天前发布
                 *   17人关注 / 29天前发布
                 *   11人关注 / 30天前发布
                 *   7人关注 / 1月前发布
                 *   1人关注 / 2月前发布
                 */
                int idx1 = followReplaceStr.indexOf("注");
                String followSize = followReplaceStr.substring(0, idx1 + 1);
                System.out.println("关注人数：" + followSize);
//                System.out.println("关注人数：" + followSize + "测试");

                String create_time = "";
                int idx2 = followReplaceStr.indexOf("/");
                int lastIdx = followReplaceStr.indexOf("布");

                int idx3 = idx2 + 1;
                while (!Character.isDigit(followReplaceStr.charAt(idx3))) {
//                    System.out.println(followReplaceStr.charAt(idx3));
                    // TODO 特殊处理【今天发布】的情况  eg: 0人关注 / 今天发布
                    if (followReplaceStr.charAt(idx3) == '今') {
                        create_time = "今天发布";
                        break;
                    } else if (followReplaceStr.charAt(idx3) == ' ') {
                        idx3++;
                    }
                }
                if (create_time == "") {
                    create_time = followReplaceStr.substring(idx3, lastIdx + 1);
                }

                // TODO 特殊处理【今天发布】的情况  eg: 0人关注 / 今天发布
                int idx4 = idx2 + 1;
                while (followReplaceStr.charAt(idx4) == ' ') {
                    idx4++;
                }

                System.out.println("发布时间：" + create_time);
//                System.out.println("发布时间：" + create_time + "测试");
                size++;
                System.out.println("获取到的条数为：" + size);
                System.out.println("-----------------------------------------------");

                info[0] = followSize;
                info[1] = create_time;

                infoList.add(info);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (String[] infoStr : infoList) {
            System.out.println(infoStr[0] + "-" + infoStr[1]);
        }

        for (String[] infoStr : infoList) {
            infoStr[0] += '|';
        }

        for (String[] infoStr : infoList) {
            System.out.println(infoStr[0] + infoStr[1]);
        }

        return infoList;
    }
}
