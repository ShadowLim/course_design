import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class RedisDemo {
//    private static String HOST = "localhost";
    private static String HOST = "10.125.0.15";
    private static int PORT = 6379;
    private static String PWD = "redis_password";
    private static Jedis jedis = null;
    private static JedisPool jedisPool = null;


    public static void main(String[] args) {
        // 1. 创建Jedis对象(两个都可以)
//        jedis = new Jedis(HOST, PORT);
        init();

        // 2. 测试
        String res = jedis.ping();
//        System.out.println(res);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            menu();
            System.out.print("please input your choice number:");
            int option = scanner.nextInt();
            switch (option) {
                case 1:
                    System.out.println("=================== 查询某个匹配key =======================");
                    System.out.print("please input the pattern key：");
                    String pattern = scanner.next();
                    queryKey(pattern);
                    break;

                case 2:
                    System.out.println("=================== 查询指定key的值 =======================");
                    System.out.print("please input the key：");
                    String key = scanner.next();
                    queryByKey(key);
                    break;

                case 3:
                    System.out.println("=================== 查询指定key和指定属性的值 =======================");
                    System.out.print("please input the key and field:");
                    String[] queryArr = new String[2];
                    for (int i = 0; i < queryArr.length; i++) {
                        queryArr[i] = scanner.next();
                    }
                    query(queryArr[0], queryArr[1]);
                    break;

                case 4:
                    System.out.println("============== 新增一条数据 ====================");
                    System.out.println("please input the key, field, value in turn:");
                    String[] queryArr1 = new String[3];
                    for (int i = 0; i < queryArr1.length; i++) {
                        queryArr1[i] = scanner.next();
                    }
                    putOne(queryArr1[0], queryArr1[1], queryArr1[2]);
                    break;

                case 5:
                    System.out.println("=================== 新增多条数据 =======================");
                    System.out.print("please input the number of data:");
                    int iNUm = scanner.nextInt();
                    System.out.println("please input the key in turn:");
                    String[] keysArr = new String[iNUm];
                    for (int i = 0; i < keysArr.length; i++) {
                        keysArr[i] = scanner.next();
                    }
                    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
                    int size = iNUm;
                    Map<String, String> map = new HashMap<String, String>();
                    System.out.println("please input the field, value in turn:");
                    while (size-- > 0) {
                        String field = scanner.next();
                        String value = scanner.next();
                        map.put(field, value);
                        list.add(map);
                    }
                    putMulti(keysArr, list);
                    break;

                case 6:
                    System.out.println("=================== 修改数据 =======================");
                    System.out.print("input the key, field, value what you want to update:");
                    String uKey = scanner.next();
                    String uField = scanner.next();
                    String uVal = scanner.next();
                    modifyData(uKey, uField, uVal);
                    break;

                case 7:
                    System.out.println("=================== 根据key删除数据 =======================");
                    System.out.println("please input the key:");
                    String dKey = scanner.next();
                    deleteOne(dKey);
                    break;

                case 8:
                    System.out.println("=================== 删除指定key和field的数据 ===================");
                    System.out.println("please input the key, field:");
                    String dKeyWithFeild = scanner.next();
                    String dFeild = scanner.next();
                    deleteField(dKeyWithFeild, dFeild);
                    break;

                case 9:
                    System.out.println("=================== 删除一组key的数据 ===================");
                    System.out.print("please input the number of data:");
                    int dNum = scanner.nextInt();
                    System.out.println("please input the keys:");
                    List<String> dList = new ArrayList<String>();
                    for (int i = 0; i < dNum; i++) {
                        dList.add(scanner.next());
                    }
                    String[] dKeysArr = new String[dList.size()];
                    for (int i = 0; i < dKeysArr.length; i++) {
                        dKeysArr[i] = dList.get(i);
                    }
                    deleteMutli(dKeysArr);
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
     * TODO 删除指定key和field的数据
     * @param key
     * @param field
     */
    public static void deleteField(String key, String field) {
        if (jedis.exists(key)) {
            if (jedis.hdel(key, field) != 0) {
                System.out.println("成功删除key为" + key + "，属性为" + field + "的这条数据！");
            } else {
                System.out.println("删除数据失败，不存在" + field + "这个属性！");
            }
        } else {
            System.out.println("删除的key[" + key + "]不存在！");
        }
        jedis.close();
    }

    /**
     * TODO 删除一组key的数据
     * @param keys
     * @return
     */
    public static void deleteMutli(String[] keys) {
        boolean[] deleted = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (jedis.exists(key)) {
                jedis.del(key);
                deleted[i] = true;
            }
            jedis.close();
        }
        int deleteSize = 0;
        for (boolean b : deleted) {
            if (b) {
                deleteSize++;
            }
        }
        if (deleteSize >= 1) {
            System.out.print("成功删除了" + deleteSize + "条数据，删除的key分别是：");
            int size = deleteSize;
            for (int i = 0; i < keys.length; i++) {
                if (deleted[i]) {
                    if (size == 1) {
                        System.out.println(keys[i]);
                    } else {
                        System.out.print(keys[i] + ", ");
                    }
                    size--;
                }
            }
        } else {
            System.out.println("这组要删除的key不存在！");
        }
    }

    /**
     * TODO 根据key删除数据
     * @param key
     */
    public static boolean deleteOne(String key) {
        if (jedis.exists(key)) {
            if (jedis.del(key) == 1) {
                System.out.println("成功删除1条数据！");
                jedis.close();
                return true;
            } else {
                System.out.println("删除数据失败");
                jedis.close();
                return false;
            }
        } else {
            System.out.println("删除的key[" + key + "]不存在！");
            jedis.close();
            return false;
        }
    }


    /**
     * TODO 修改数据
     * @param key
     * @param value
     * @return
     */
    public static boolean modifyData(String key, String field, String value) {
        if (jedis.exists(key)) {
            jedis.hset(key, field, value);
            if (value.equals(jedis.hget(key, field))) {
                System.out.println("键为" + key + "，属性为" + field + "的数据" + "修改成功！");
                jedis.close();
                return true;
            } else {
                System.out.println("修改数据失败");
                jedis.close();
                return false;
            }
        } else {
            System.out.println(key + "不存在");
            jedis.close();
            return false;
        }
    }

    /**
     * TODO 新增多条数据
     * @param keysArr
     * @param fieldValues
     */
    public static void putMulti(String[] keysArr, List<Map<String, String>> fieldValues) {

        // TODO 设置多个 k-v
//        jedis.hmset("k1", "v1", "f1", "k2", "v2", "f2");

        int length = keysArr.length;
        for (int i = 0; i < keysArr.length; i++) {
//            Map<String, String> map = fieldValues.get(i);
//            String field = "", value = "";
//            for (Map.Entry<String, String> entry : map.entrySet()) {
//                field = entry.getKey();
//                value = entry.getValue();
//            }
            jedis.hmset(keysArr[i], fieldValues.get(i));
        }
        System.out.println("数据新增成功！");
        jedis.close();
    }


    /**
     * TODO 新增一条数据
     */
    public static void putOne(String key, String field, String value) {
        jedis.hset(key, field, value);
        String values = jedis.hget(key, field);
        System.out.println("新增了一条键为" + key + "，属性为" + field + "值为：" + values + "的数据");
        jedis.close();
    }

    /**
     * TODO 查询指定key和指定属性的值
     * @param key
     * @param field
     */
    public static void query(String key, String field) {
        Boolean vis = jedis.exists(key);
        if (vis) {
            String value = jedis.hget(key, field);
            if ("".equals(value)) {
                System.out.println("键为" + key + "不存在"  + "属性" + field);
            } else {
                System.out.println("键为" + key + "的" + field + "属性值为：" + value);
            }
        } else {
            System.out.println("你所查询的key不存在！");
        }
        jedis.close();
    }

    /**
     * TODO 查询指定key的值
     * @param key
     */
    public static void queryByKey(String key) {
        Boolean vis = jedis.exists(key);
        if (vis) {
            Map<String, String> valueMap = jedis.hgetAll(key);
            int size = valueMap.size();
            System.out.println("键为" + key + "的查询大小为：" + size);
            System.out.println("键为" + key + "的查询结果为：");
            System.out.println("{");
            for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                if (size == 1) {
                    System.out.println("\t[" + entry.getKey() + "," + entry.getValue() + "]");
                } else {
                    System.out.println("\t[" + entry.getKey() + "," + entry.getValue() + "],");
                }
                size--;
            }
            System.out.println("}");
        } else {
            System.out.println("你所查询的key不存在！");
        }
        jedis.close();
    }


    /**
     * TODO 查询某个匹配key
     * @param pattern
     */
    public static void queryKey(String pattern) {
        Set<String> keys = jedis.keys(pattern);
        int cnt = 0;
        boolean flag = true;
        int size = keys.size();
        Iterator<String> iterator = keys.iterator();
        System.out.println("经查询匹配到的key有" + size + "个");
        if (size <= 10) {
            System.out.println("分别为：");
            for (String key : keys) {
                System.out.print(key + " ");
            }
            System.out.println();
        } else {
            System.out.println("前5个key分别为：");
            while (iterator.hasNext() && flag) {
                String key = iterator.next();
                cnt++;
                System.out.print(key + " ");
                if (cnt > 5) {
                    flag = false;
                }
            }
            System.out.println();
            System.out.println("后5个key分别为：");
            for (int i = 0; i < 5; i++) {
                String key = (String) keys.toArray()[size - i - 1];
                System.out.print(key + " ");
            }
            System.out.println();
        }
    }


    /**
     * TODO 获取Jedis实例
     */
    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                return null;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * TODO 释放资源
     */
    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
//            jedisPool.returnResource(jedis);
            jedis.close();
            jedisPool.close();
        }
    }


    /**
     * TODO 初始化Redis连接池
     */
    public static void init() {
        if (jedis == null) {
            jedis = new Jedis(HOST, PORT);
            jedis.auth(PWD);
        }
        if (jedis != null) {
            System.out.println("Redis连接成功");
        } else {
            System.out.println("Redis连接失败");
        }
    }

    public static void menu() {
        System.out.println("|----------------------Redi增删改查操作-------------------------|");
        System.out.println("|----------------- 1.查询某个匹配key ---------------------------|");
        System.out.println("|----------------- 2.查询指定key的值 ---------------------------|");
        System.out.println("|----------------- 3.查询指定key和指定属性的值 ------------------|");
        System.out.println("|----------------- 4.新增一条数据 ------------------------------|");
        System.out.println("|----------------- 5.新增多条数据 ------------------------------|");
        System.out.println("|----------------- 6.修改数据 ----------------------------------|");
        System.out.println("|----------------- 7.根据key删除数据 ---------------------------|");
        System.out.println("|----------------- 8.删除指定key和field的数据 -------------------|");
        System.out.println("|----------------- 9.删除一组key的数据 --------------------------|");
        System.out.println("|----------------- 0.退出系统 ----------------------------------|");
        System.out.println("|--------------------------------------------------------------|");
    }
}
