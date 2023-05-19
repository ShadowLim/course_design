import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.*;


public class MongoDBDemo {

    private static String dbName = "db_books";
    private static String collectionName = "tb_books";
    private static MongoClient client;
    private static MongoDatabase db;
    private static MongoCollection<Document> collection;


    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            init();
            menu();
            System.out.print("please input your choice number:");
            int option = scanner.nextInt();
            switch (option) {
                case 1:
                    System.out.println("=================== 查询前5条数据 =======================");
                    queryTop5();
                    break;

                case 2:
                    System.out.println("=================== 按属性查询数据 =======================");
                    System.out.print("please input the fieldName, fieldValue：");
                    String fieldName = scanner.next();
                    String fieldValue = scanner.next();
                    queryOne(fieldName, fieldValue);
                    break;

                case 3:
                    System.out.println("=================== 插入一条数据 =======================");
                    System.out.print("please input the value what you want to insert:");
                    String[] iArr = new String[10];
                    for (int i = 0; i < iArr.length; i++) {
                        iArr[i] = scanner.next();
                    }
                    insertOne(iArr);
                    break;

                case 4:
                    System.out.println("============== 批量插入数据 ====================");
                    System.out.print("please input the number of data:");
                    int iSize = scanner.nextInt();
                    System.out.println("please input the key, field, value in turn:");
                    List<String[]> iBatchList = new ArrayList<String[]>();
                    String[] ibArr;
                    int size = iSize;
                    while (size-- > 0) {
                        ibArr = new String[10];
                        for (int i = 0; i < ibArr.length; i++) {
                            ibArr[i] = scanner.next();
                        }
                        iBatchList.add(ibArr);
                    }
//                    for (String[] strings : iBatchList) {
//                        // print id
//                        System.out.println(strings[0]);
//                    }
                    insertBatch(iBatchList);
                    break;

                case 5:
                    System.out.println("=================== 修改一条数据 =======================");
                    System.out.println("please input the feildName, feildValue, newValue:");
                    String uFeildName = scanner.next();
                    String uFeildValue = scanner.next();
                    String unewValue = scanner.next();
                    updateOne(uFeildName, uFeildValue, unewValue);
                    break;

                case 6:
                    System.out.println("=================== 修改多条数据 =======================");
                    System.out.print("please input the number of data to update:");
                    int uSize = scanner.nextInt();
                    List<String[]> uMutliList = new ArrayList<String[]>();
                    String[] line;
                    System.out.println("please input the feildName, feildValue, newValue in trun:");
                    for (int i = 0; i < uSize; i++) {
                        line = new String[3];
                        line[0] = scanner.next();
                        line[1] = scanner.next();
                        line[2] = scanner.next();
                        uMutliList.add(line);
                    }
                    updateMulti(uMutliList);
                    break;

                case 7:
                    System.out.println("=================== 删除一个文档 ===================");
                    System.out.println("please input the feildName, feildValue:");
                    String dFeildName = scanner.next();
                    String dFeildValue = scanner.next();
                    dropOne(dFeildName, dFeildValue);
                    break;

                case 8:
                    System.out.println("=================== 删除多个文档 ===================");
                    System.out.print("please input the number of data to delete:");
                    int dmSize = scanner.nextInt();
                    System.out.println("please input the feildName, feildValue:");
                    List<String[]> umList = new ArrayList<String[]>();
                    String[] dmDocumets;
                    for (int i = 0; i < dmSize; i++) {
                        dmDocumets = new String[2];
                        dmDocumets[0] = scanner.next();
                        dmDocumets[1] = scanner.next();
                        umList.add(dmDocumets);
                    }
                    dropMany(umList);
                    break;

                case 0 :
                    System.out.println("Thank you for using the system!");
                    System.exit(0);

                default :
                    System.out.println("The number what you choose is error!");
            }
        }
    }


    /**
     * TODO 删除多个文档
     * @param list
     */
    public static void dropMany(List<String[]> list) {
        int size = 0;
        for (String[] line : list) {
            String fieldName = line[0];
            String fieldValue = line[1];
            Bson filter = Filters.eq(fieldName, fieldValue);
            try {
                DeleteResult result = collection.deleteMany(filter);
                size += result.getDeletedCount();
//                System.out.println("Deleted document count: " + result.getDeletedCount());
            } catch (MongoException me) {
                System.err.println("Unable to delete due to an error: " + me);
            }
        }
        System.out.println("Deleted document count: " + size);
        client.close();
    }

    /**
     * TODO 删除一个文档
     * @param fieldName
     * @param fieldValue
     */
    public static void dropOne(String fieldName, String fieldValue) {
        // TODO delete one document
        //  fieldName = name，fieldValue = 老舍

        Bson exists = exists(fieldName);
        // 申明删除条件
        Bson filter = eq(fieldName, fieldValue);

        try {
            DeleteResult result = collection.deleteOne(filter);
            System.out.println("Deleted document count: " + result.getDeletedCount());
        } catch (MongoException me) {
            System.err.println("Unable to delete due to an error: " + me);
        }
        client.close();
    }


    /**
     * TODO 修改多条数据
     * @param list
     */
    public static void updateMulti(List<String[]> list) {
        // update one document
        int size = 0;
        for (String[] line : list) {
            String fieldName = line[0];
            String fieldValue = line[1];
            String newValue = line[2];
            Bson filter = eq(fieldName, fieldValue);

//            Bson combine = Updates.combine(Updates.addToSet(fieldName, newValue));
//            UpdateResult updateResult = collection.updateMany(filter, combine);
//            System.out.println("Modified document count: " + updateResult.getModifiedCount());
            UpdateResult updateResult = collection.updateMany(filter, new Document("$set", new Document(fieldName, newValue)));
            size += updateResult.getModifiedCount();
        }
        System.out.println("update " + size + " document successfully!");
        client.close();
    }

    /**
     * TODO 修改一条数据
     * @param fieldName
     * @param fieldValue
     * @param newValue
     */
    public static void updateOne(String fieldName, String fieldValue, String newValue) {
        // update one document
        Bson filter = eq(fieldName, fieldValue);
        try {
            collection.updateOne(filter, new Document("$set", new Document(fieldName, newValue)));
            System.out.println("update one document successfully!");
        } catch (MongoException me) {
            System.err.println("Unable to update due to an error: " + me);
        }
        client.close();
    }

    /**
     * TODO 批量插入数据
     * @param valuesList
     */
    public static void insertBatch(List<String[]> valuesList) {
        int size = 1;
        List<Document> list = new ArrayList<Document>();
        for (String[] values : valuesList) {
            list.add(new Document()
                    .append("id", values[0])
                    .append("type", values[1])
                    .append("name", values[2])
                    .append("author", values[3])
                    .append("price", values[4])
                    .append("discount", values[5])
                    .append("pub_time", values[6])
                    .append("pricing", values[7])
                    .append("publisher", values[8])
                    .append("crawler_time", values[9])
            );
            if (list.size() == 2) {
                try {
                    collection.insertMany(list);
//                    System.out.println("--------------已插入一个批次：2条");
                    size *= 2;
                    list.clear();
                } catch (MongoException me) {
                    System.err.println("Unable to insert due to an error: " + me);
                }
            }
        }
        if (!list.isEmpty()) {
            try {
                collection.insertMany(list);
//                System.out.printf("--------------已插入一个批次：%s条", list.size());
//                System.out.println();
                size += 1;
                list.clear();
            } catch (MongoException me) {
                System.err.println("Unable to insert due to an error: " + me);
            }
        }
        System.out.println("Inserted" + size + " document successfully!");
        client.close();
    }

    /**
     * TODO 插入一条数据
     * @param values
     */
    public static void insertOne(String[] values) {
        try {
            Document document = new Document()
                    .append("id", values[0])
                    .append("type", values[1])
                    .append("name", values[2])
                    .append("author", values[3])
                    .append("price", values[4])
                    .append("discount", values[5])
                    .append("pub_time", values[6])
                    .append("pricing", values[7])
                    .append("publisher", values[8])
                    .append("crawler_time", values[9]);
            collection.insertOne(document);
            System.out.println("document insert successfully!");
        } catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        }
        client.close();
    }


    /**
     * TODO 按属性查询数据
     * @param fieldName
     * @param fieldValue
     */
    public static void queryOne(String fieldName, String fieldValue) {
        int size = 0;
        Bson filter = Filters.eq(fieldName, fieldValue);
        FindIterable findIterable = collection.find(filter);
        MongoCursor cursor = findIterable.iterator();

        try {
            if (!cursor.hasNext()) {
                System.out.println("Don't find what you want to query!");
            }
            while (cursor.hasNext()) {
                /**
                 * Document{{_id=637ab900a52c2a5baa611dc0, id=1, type=小说, name=我这一辈子:老舍中短篇小说集, author=老舍, price=17.3, discount=4.8折, pub_time=2019-09-01, pricing=36.0, publisher=中国华侨出版社, crawler_time=2022-10-27 22:25:27}}
                 */
//                cursor.next().toJson()
                String next = cursor.next().toString();
                String[] split = next.split(",");
                for (int i = 0; i < split.length; i++) {
                    if (i == 0) {
                        String prefix = split[i].substring(0, 9);
                        System.out.println(prefix);
                        System.out.println(" " + split[i].substring(10, 14) + "ObjectId(\"" +
                                split[i].substring(14) + "\")" + ",");
                    } else if (i < split.length - 1){
                        System.out.println(split[i] + ",");
                    } else {
                        System.out.println(split[i].substring(0, split[i].length() - 2));
                    }
                }
                size++;
                if (size == 1 && !cursor.hasNext()) {
                    System.out.println("}");
                } else {
                    System.out.println("},");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * TODO 查询前5条数据
     */
    public static void queryTop5() {
        FindIterable<Document> documents = collection.find().limit(5);
        for (Document document : documents) {
            System.out.println(document);
        }
        client.close();
    }

    public static void init() {
        try {
            client = new MongoClient("10.125.0.15");
//            client = new MongoClient("localhost");
            db = client.getDatabase(dbName);
            collection = db.getCollection(collectionName);
            // TODO 设置console日志级别
            Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
            mongoLogger.setLevel(Level.SEVERE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void menu() {
        System.out.println("|----------------------- MongoDB增删改查操作 ----------------------------|");
        System.out.println("|----------------- 1.查询前5条数据 --------------------------------------|");
        System.out.println("|----------------- 2.按属性查询数据 --------------------------------------|");
        System.out.println("|----------------- 3.插入一条数据 ---------------------------------------|");
        System.out.println("|----------------- 4.批量插入数据 ---------------------------------------|");
        System.out.println("|----------------- 5.修改一条数据 ---------------------------------------|");
        System.out.println("|----------------- 6.修改多条数据 ---------------------------------------|");
        System.out.println("|----------------- 7.删除一个文档 ---------------------------------------|");
        System.out.println("|----------------- 8.删除多个文档 ---------------------------------------|");
        System.out.println("|----------------- 0.退出系统 ------------------------------------------|");
        System.out.println("|----------------------------------------------------------------------|");
    }
}