package cn.lbj.house.service;

import cn.lbj.house.util.DBUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class GetHouseInfo {

    private static DBUtil dbUtil = new DBUtil();
    private static Connection conn = null;
    private static Statement statement = null;
    private static ResultSet rs = null;
    private static String sql = "";

    private static PreparedStatement ps = null;


    public static void main(String[] args) {
        getRealHouseInfo();

//        List<String[]> newHouseInfo = getRealHouseInfo();
//        updateHouseInfo(newHouseInfo);

    }


    public static void updateHouseInfo(List<String[]> newHouseInfo) {
        try {
            conn = dbUtil.getConnection();
            int cnt = 1;
            for (String[] newInfoArr : newHouseInfo) {
                String newInfo = "";
                for (String subInfo : newInfoArr) {
                    newInfo += subInfo;
                }
                System.out.println("新的字段信息为：" + newInfo);
                sql = "update tb_house set houseInfo = " + "'" + newInfo + "'" + " where id = " + cnt;
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



    public static List<String[]> getRealHouseInfo() {
        List<String[]> infoList = new ArrayList<String[]>();

        try {
            conn = dbUtil.getConnection();
            sql = "select houseInfo from tb_house";
//            sql = "select houseInfo from tb_house limit 6020, 1";

            System.out.println("sql语句：" + sql);
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                String houseStr = rs.getString(1);
                String houseReplaceStr = houseStr.replace(" ", "");
                System.out.println("replace空格处理后的信息字段为：");
                System.out.println(houseReplaceStr);
                System.out.print("replace空格处理后的信息字段长度为：");
                System.out.println(houseReplaceStr.length());
                System.out.println("*******************************");

                /**
                 * 楼层：低楼层
                 * 层数：共7层
                 * 建造年份：1994年建
                 * 规格：3室1厅
                 * 面积大小：77.4平米
                 * 房子走向:南北
                 */

                // TODO 获取楼层信息（楼层信息有且仅有2/3个字符） eg:中/低/高楼层 顶层 底层 地下室
                String floor = houseReplaceStr.substring(0, 3);
                if (floor.equals("地下室")) {
                    floor = "地下室";
                } else if (!floor.contains("楼层")) {
                    floor = houseReplaceStr.substring(0, 2);
                }
                System.out.println("楼层：" + floor);
//                System.out.println("楼层：" + floor + "测试");

                // TODO 获取楼层层数
                int lidx = houseReplaceStr.indexOf('(');
                int ridx = houseReplaceStr.indexOf(')');
                String layerSize = houseReplaceStr.substring(lidx + 1, ridx);
                System.out.println("层数：" +layerSize);
//                System.out.println("层数：" +layerSize + "测试");

                // TODO 1993年建(有且仅有6个字符)
                int newidx1 = houseReplaceStr.indexOf('|');
                String startYear = houseReplaceStr.substring(newidx1 + 1, newidx1 + 7);
                // TODO 对于该字段中不存在“建造年份”的信息  均采用填充“未透露年份建”
                if (startYear.length() != 6 || !startYear.contains("年建")) {
                    startYear = "未透露年份建";
                }
                System.out.println("建造年份："  + startYear);
//                System.out.println("建造年份："  + startYear + "测试");


                // TODO 1室1厅(该信息有且仅有4个字符)
                //  eg: 地下室 (共1层) 1室0厅|12.2平米|南
                int newidx2 = houseReplaceStr.indexOf('室');
                int anotherNewIdx2 = houseReplaceStr.indexOf('室', newidx2 + 1);
                System.out.println("==========================下一个'室'下标为：" + anotherNewIdx2);

                String scale = "";
                if (anotherNewIdx2 == -1) {
                    scale = houseReplaceStr.substring(newidx2 - 1, newidx2 + 3);
                } else {
                    scale = houseReplaceStr.substring(anotherNewIdx2 - 1, anotherNewIdx2 + 3);
                }
                System.out.println("规格：" + scale);
//                System.out.println("规格：" + scale + "测试");

                // TODO 34.94平米(该信息含有特征字符'.')
                //  90平米(该信息不含有特征字符'.')
                int newidx3 = houseReplaceStr.indexOf('.');
//                System.out.println("特殊字符." + newidx3);
                int otherIdx = houseReplaceStr.indexOf('平');
                int newidx4 = houseReplaceStr.indexOf('米');

                String size = "";
                if (newidx3 == -1) {    // TODO 90平米
                    int newidx5 = otherIdx;
                    // TODO 防止截取到空格字符，需要判断newidx3前的字符算法为数字，是则属于面积大小的信息，否则获取到面积大小该信息的起始位置
                    while (Character.isDigit(houseReplaceStr.charAt(newidx5 - 1))) {
//                        System.out.println(houseReplaceStr.charAt(newidx5 - 1));
                        newidx5--;
                    }
                    size = houseReplaceStr.substring(newidx5, newidx4 + 1);
                    System.out.println("面积大小：" + size);
//                System.out.println("面积大小：" + size + "测试");
                } else {            // TODO 104.35平米
                    int newidx5 = newidx3;
                    // TODO 防止截取到空格字符，需要判断newidx3前的字符算法为数字，是则属于面积大小的信息，否则获取到面积大小该信息的起始位置
                    while (Character.isDigit(houseReplaceStr.charAt(newidx5 - 1))) {
//                        System.out.println(houseReplaceStr.charAt(newidx5 - 1));
                        newidx5--;
                    }
                    size = houseReplaceStr.substring(newidx5, newidx4 + 1);
                    System.out.println("面积大小：" + size);
//                System.out.println("面积大小：" + size + "测试");
                }


                String lastStr = houseReplaceStr.substring(newidx4);
                int newidx6 = lastStr.indexOf('|');
                int startIdx1 = newidx6;
                if (houseReplaceStr.charAt(startIdx1 + 1) == ' ') {
                    startIdx1++;
                }
                String trend = lastStr.substring(startIdx1 + 1).replace(" ", "");
                System.out.println("房子走向:" + trend);
//                System.out.println("房子走向:" + trend + "测试");
                System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");

                String[] infoArr = new String[6];
                infoArr[0] = floor;
                infoArr[1] = layerSize;
                infoArr[2] = startYear;
                infoArr[3] = scale;
                infoArr[4] = size;
                infoArr[5] = trend;
                infoList.add(infoArr);
                System.out.println("-----------------------------------------" +
                        "------获取到的条数为：" + infoList.size());

                // TODO 房子走向不存在为空的信息
                //  通过控制台的打印结果可知
                if (infoArr[5] == "") {
                    System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("-------------------------------------------");
        /**
         * 顶层-共6层-1993年建-1室1厅-34.94平米-南!
         * 底层-共6层-1993年建-1室1厅-34.94平米-南!
         * 中楼层-共6层-1993年建-1室1厅-34.94平米-南!
         * 低楼层-共6层-1995年建-2室1厅-59.71平米-南!
         * 高楼层-共6层-1995年建-2室1厅-67.4平米-南!
         * 低楼层-共7层-1994年建-3室1厅-77.4平米-南北!
         */
        for (String[] info : infoList) {
            System.out.println(info[0] + "-" + info[1] + "-" + info[2] + "-" + info[3] + "-" +
                    info[4] + "-" + info[5] + "!");
        }

        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");

        for (String[] info : infoList) {
            info[0] += "|";
            info[1] += "|";
            info[2] += "|";
            info[3] += "|";
            info[4] += "|";
        }
        /**
         * 顶层|共11层|2006年建|1室0厅|47.7平米|北
         * 高楼层|共18层|未透露年份建|2室1厅|63平米|东南
         * 低楼层|共25层|未透露年份建|3室2厅|119.35平米|西南
         * 低楼层|共23层|2004年建|3室2厅|128.15平米|南
         * 中楼层|共18层|1996年建|3室1厅|84.05平米|东南
         */
        for (String[] info : infoList) {
            System.out.println(info[0] + info[1] + info[2] + info[3] +
                    info[4] + info[5]);
        }

        return infoList;
    }
}
