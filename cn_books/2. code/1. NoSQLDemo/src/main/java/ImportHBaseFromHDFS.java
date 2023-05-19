import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ImportHBaseFromHDFS {
    private static int writeSize = 0;

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
//         conf.set("hbase.zookeeper.quorum", "10.125.0.15:2181");

        conf.set(TableOutputFormat.OUTPUT_TABLE, "tb_books");
//        conf.set(TableOutputFormat.OUTPUT_TABLE, "books_tmp");
        Job job = Job.getInstance(conf, ImportHBaseFromHDFS.class.getSimpleName());
        TableMapReduceUtil.addDependencyJars(job);

        job.setJarByClass(ImportHBaseFromHDFS.class);
        job.setMapperClass(HdfsToHBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        System.out.println("导入数据的id为：");
        job.setReducerClass(HdfsToHBaseReducer.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://10.125.0.15:9000/books_cn/tb_book.txt"));
//        FileInputFormat.addInputPath(job, new Path("hdfs://10.125.0.15:9000/books_cn/test.txt"));
        job.setOutputFormatClass(TableOutputFormat.class);

        job.waitForCompletion(true);
        System.out.println();

        System.out.println("成功往HBase导入" + writeSize + "条数据！");
    }

    public static class HdfsToHBaseMapper extends Mapper<Object, Text, Text, Text> {
        private Text outk = new Text();
        private Text outv = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
//            System.out.println("id:" + splits[0]);
            outk.set(splits[0]);
            outv.set(splits[1] + "\t" + splits[2] + "\t" + splits[3] + "\t"
                    + splits[4] + "\t" + splits[5] + "\t" + splits[6] + "\t" + splits[7] + "\t"
                    + splits[8] + "\t" + splits[9]);
            context.write(outk, outv);
        }
    }


    public static class HdfsToHBaseReducer extends TableReducer<Text, Text, NullWritable> {
        private boolean[] flag = new boolean[8];

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.getBytes());  // rowkey
            String idStr = key.toString();
            String columnFamily = "info";

            writeSize++;

            if (idStr != null && !"NULL".equals(idStr)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(idStr));

                if (idStr.startsWith("1")) {
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("2")) {
                    if (!flag[0]) {
                        System.out.println();
                        flag[0] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("3")) {
                    if (!flag[1]) {
                        System.out.println();
                        flag[1] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("4")) {
                    if (!flag[2]) {
                        System.out.println();
                        flag[2] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("5")) {
                    if (!flag[3]) {
                        System.out.println();
                        flag[3] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("6")) {
                    if (!flag[4]) {
                        System.out.println();
                        flag[4] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("7")) {
                    if (!flag[5]) {
                        System.out.println();
                        flag[5] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("8")) {
                    if (!flag[6]) {
                        System.out.println();
                        flag[6] = true;
                    }
                    System.out.print(idStr + " ");
                } else if (idStr.startsWith("9")) {
                    if (!flag[7]) {
                        System.out.println();
                        flag[7] = true;
                    }
                    System.out.print(idStr + " ");
                } else {
                    System.out.println();
                }
            }

            for (Text value : values) {
                String[] line = value.toString().split("\t");
                // TODO info，对应hbase列族名
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(line[1])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("author"), Bytes.toBytes(String.valueOf(line[2])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("price"), Bytes.toBytes(line[3]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("discount"), Bytes.toBytes(String.valueOf(line[4])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("pub_time"), Bytes.toBytes(line[5]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("pricing"), Bytes.toBytes(line[6]));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("publisher"), Bytes.toBytes(String.valueOf(line[7])));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("crawler_time"), Bytes.toBytes(line[8]));
                if ("".equals(idStr) || "".equals(line[1]) || "".equals(line[2]) || "".equals(line[3]) ||
                        "".equals(line[4]) || "".equals(line[5]) || "".equals(line[6]) || "".equals(line[7]) ||
                        "".equals(line[8])) {
                    System.out.println("-----------------" + idStr);
                }
            }
            context.write(NullWritable.get(), put);
        }
    }
}
