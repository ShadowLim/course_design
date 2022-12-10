import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


// TODO 13518条数据
public class OperateHBase {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    private static int ID = 13519;
        private static String tableName = "tb_books";
//    private static String tableName = "books_tmp";

    public static void main(String[] args) throws Exception {

        Scanner scanner = new Scanner(System.in);

        while (true) {
            menu();
            System.out.print("please input your choice number:");
            int option = scanner.nextInt();
            switch (option) {
                case 1:
                    System.out.println("=================== 查询数据行数 =======================");
                    countRows(tableName);
                    break;

                case 2:
                    System.out.println("=================== Scan全表数据 =======================");
                    getResultScann(tableName);
                    break;

                case 3:
                    System.out.println("=================== 根据RowKey查找数据 =======================");
                    System.out.print("please input the rowKey:");
                    String queryRK = scanner.next();
                    getDataByRowKey(tableName, queryRK);
                    break;

                case 4:
                    System.out.println("=================== 根据rowKey和colFamily查询 =======================");
                    System.out.print("please input the rowKey, colFamily:");
                    String rowKey = scanner.next();
                    String colFamily = scanner.next();
                    scanColumn(tableName, rowKey, colFamily);
                    break;

                case 5:
                    System.out.println("============== 查询具体某一列族的某一列的信息 ====================");
                    System.out.println("please input the rowKey, colFamily, column in turn:");
                    String[] queryArr1 = new String[3];
                    for (int i = 0; i < queryArr1.length; i++) {
                        queryArr1[i] = scanner.next();
                    }
                    getData(tableName, queryArr1[0], queryArr1[1], queryArr1[2]);
                    break;

                case 6:
                    System.out.println("=================== 插入具体一列数据 =======================");
                    System.out.println("please input the rowKey, colFamily, column, value in turn:");
                    String[] infos = new String[4];
                    for (int i = 0; i < infos.length; i++) {
                        infos[i] = scanner.next();
                    }
                    insertColumn(tableName, infos[0], infos[1], infos[2], infos[3]);
                    break;

                case 7:
                    System.out.println("=================== 插入一行数据 =======================");
                    String[] fields = {
                            "info:id", "info:type", "info:name", "info:author", "info:price",
                            "info:discount", "info:pub_time", "info:pricing", "info:publisher",
                            "info:crawler_time"
                    };
                    System.out.print("input the rowkey what you want to insert:");
                    String rowkey = scanner.next();
                    String[] values = new String[10];
                    System.out.print("please input the row's data:");
                    for (int i = 0; i < values.length; i++) {
                        values[i] = scanner.next();
                    }
                    addRecord(tableName, rowkey, fields, values);
                    break;

                case 8:
                    System.out.println("=================== 修改指定列数据 =======================");
                    System.out.println("please input the rowkey, column, value in turn:");
                    // TODO tb_books 1002 info:price
                    String[] info = new String[3];
                    for (int i = 0; i < info.length; i++) {
                        info[i] = scanner.next();
                    }
                    modifyData(tableName, info[0], info[1], info[2]);
                    break;

                case 9:
                    System.out.println("=================== 删除一行数据 =======================");
                    System.out.println("please input the rowKey:");
                    String dRowKey = scanner.next();
                    deleteRow(tableName, dRowKey);
                    break;

                case 10:
                    System.out.println("=================== 删除指定列族的数据 =======================");
                    System.out.println("please input the rowKey, colFamily in turn:");
                    String[] deleteinfos = new String[2];
                    for (int i = 0; i < deleteinfos.length; i++) {
                        deleteinfos[i] = scanner.next();
                    }
                    deleteByCF(tableName, deleteinfos[0], deleteinfos[1]);
                    break;

                case 0 :
                    System.out.println("Thank you for using the system!");
                    System.exit(0);

                default :
                    System.out.println("The number what you choose is error!");
            }
        }

    }


    // TODO 删除指定rowKey
    public static void deleteRow(String tableName, String rowKey) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete data = new Delete(rowKey.getBytes());
        table.delete(data);
        System.out.println("delete rowKey " + rowKey + "'s" + " data successfully!");
        table.close();
        close();
    }

    // TODO 删除colFamily
    public static void deleteByCF(String tableName, String rowKey, String colFamily) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete data = new Delete(rowKey.getBytes());
        data.addFamily(Bytes.toBytes(colFamily));
        table.delete(data);
        System.out.println("Deleted RowKey:[" +  rowKey +",ColFamily:[" + colFamily + "]]'s " + " data successfully!");
        table.close();
        close();
    }

    /**
     * TODO 修改表 tableName，行row，列column指定的单元格的数据。
     * @param tableName
     * @param row
     * @param column
     * @param val
     * @throws IOException
     */
    public static void modifyData(String tableName, String row, String column, String val) throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        String[] cols = column.split(":");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(cols[0]));
        int size = 0;
        ResultScanner scanner = table.getScanner(scan);
        if (scanner.next() == null) {
            Get get = new Get(row.getBytes());
            Result result = table.get(get);
            if (result == null) {
                System.out.println("当前修改的RowKey:" + row + "不存在！");
            } else {
                System.out.println("您要修改的列不存在！");
            }
        } else {
            if (scanner.next() != null){
                put.addColumn(cols[0].getBytes(), cols[1].getBytes(), val.getBytes());
                table.put(put);
                size++;
            }
            System.out.println("成功修改" + "列为" + column + "共" + size + "条！");
        }
        table.close();
        close();
    }

    /**
     * TODO 插入一行数据
     * @param tableName
     * @param row
     * @param fields
     * @param values
     * @throws IOException
     */
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i != fields.length; i++) {
            Put put = new Put(row.getBytes());
            String[] cols = fields[i].split(":");
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        System.out.println("add record successfully!");
        table.close();
        close();
    }

    // TODO 插入具体一列的值
    public static void insertColumn(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put data = new Put(rowKey.getBytes());
        data.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(data);
        System.out.println("insert data successfully!");
        table.close();
        close();
    }

    /**
     * TODO 根据rowKey和colFamily查询
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @throws IOException
     */
    public static void scanColumn(String tableName, String rowKey, String colFamily) throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(colFamily));
        scan.setStartRow(Bytes.toBytes(rowKey));
        scan.setStopRow(Bytes.toBytes(rowKey));
        ResultScanner scanner = table.getScanner(scan);

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            showCell(result);
        }

        // null
//        System.out.println(scanner.next());
//        if (scanner.next() == null) {
//            System.out.println("No Return!");
//        }
//        for (Result result = scanner.next(); result != null; result = scanner.next()){
//            if (result != null) {
//                showCell(result);
//            }
//            System.out.println(result);
//            if (result.toString().contains("NONE")) {
//                System.out.println("No Return!");
//            }
//        }
        table.close();
        close();
    }


    /**
     * TODO 根据具体某一列族的某一列查找数据
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String colFamily, String col)throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        Result result = table.get(get);
        if (result != null) {
            showCell(result);
        }
        if (result.toString().contains("NONE")) {
            System.out.println("No Return!");
        }
        table.close();
        close();
    }

    /**
     * TODO 根据行键查找数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getDataByRowKey(String tableName, String rowKey)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        // keyvalues={rk001/info:id/1669648973007/Put/vlen=5/seqid=0}
        // keyvalues=NONE
//        System.out.println(result.toString());
        if (result != null) {
            showCell(result);
        }
        if (result.toString().contains("NONE")) {
            System.out.println("当前查找的RowKey:" + rowKey + "不存在数据！");
        }
        table.close();
        close();
    }


    /**
     * TODO 扫描全表
     * @param tableName
     * @return
     * @throws Exception
     */
    public static ResultScanner getResultScann(String tableName) throws Exception {
        init();
        TableName name = TableName.valueOf(tableName);
        ResultScanner scanner;
        int size = 0;
        if (admin.tableExists(name)) {
            Table table = connection.getTable(name);
            Scan scan = new Scan();
            scanner = table.getScanner(scan);
            for (Result result : scanner) {
                showCell(result);
//                Cell[] cells = result.rawCells();
//                for (Cell cell : cells) {
//                    System.out.print("行健: " + new String(CellUtil.cloneRow(cell)) + "[");
//                    System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)) + ",");
//                    System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)) + ",");
//                    System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)) + ",");
//                    System.out.print(" 时间戳: " + cell.getTimestamp() + "]");
//                }
                size++;
            }
            System.out.println("扫描全表数据大小：" + size + "条！");
        } else {
            scanner = null;
        }
        return scanner;
    }

    /**
     * TODO 统计表的行数
     * @param tableName
     * @throws IOException
     */
    public static void countRows(String tableName)throws IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int num = 0;
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            num++;
        }
        System.out.println("行数:"+ num);
        scanner.close();
        close();
    }

    /**
     * TODO 统计表的行数
     * @param result
     * @throws IOException
     */
    public static void showCell(Result result) throws IOException {
        for (Cell cell : result.rawCells()) {
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)) + ",");
            System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)) + ",");
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)) + ",");
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)) + ",");
            System.out.println(" 时间戳: " + cell.getTimestamp());
        }
    }


    /**
     * TODO 建立连接
     */
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://10.125.0.15:9000/hbase");
//        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO 关闭连接
     */
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if(null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void menu() {
        System.out.println("|------------------------ HBase增删改查操作 -------------------------------|");
        System.out.println("|-------------------- 1. 查询数据行数 -------------------------------------|");
        System.out.println("|-------------------- 2. Scan全表 ----------------------------------------|");
        System.out.println("|-------------------- 3. 根据RowKey查找数据 -------------------------------|");
        System.out.println("|-------------------- 4. 根据rowKey和colFamily查询 ------------------------|");
        System.out.println("|-------------------- 5. 查询具体某一列族的某一列的信息 ----------------------|");
        System.out.println("|-------------------- 6. 插入具体一列数据 ----------------------------------|");
        System.out.println("|-------------------- 7. 插入一行数据 -------------------------------------|");
        System.out.println("|-------------------- 8. 修改指定列数据 ------------------------------------|");
        System.out.println("|-------------------- 9. 删除一行数据 --------------------------------------|");
        System.out.println("|-------------------- 10.根据colFamily删除 --------------------------------|");
        System.out.println("|-------------------- 0. 退出程序 -----------------------------------------|");
        System.out.println("|-------------------------------------------------------------------------|");
    }
}
