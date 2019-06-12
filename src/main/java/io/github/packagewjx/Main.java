package io.github.packagewjx;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.splitter.StandaloneMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        InputStream fin = Main.class.getClassLoader().getResourceAsStream("db.properties");
        if (fin == null) {
            System.out.println("no db.properties");
            System.exit(1);
        }
        properties.load(fin);
        String mongoHost = properties.getProperty("mongoHost");
        fin.close();

        Configuration conf = new Configuration();
        // 使用测试的数据库gmw
        MongoConfigUtil.setInputURI(conf, "mongodb://" + mongoHost + "/guangmingNews.gmw");
        MongoConfigUtil.setSplitterClass(conf, StandaloneMongoSplitter.class);
//        MongoConfigUtil.setOutputURI(conf, mongoURI + "/guangmingNews.word");
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Main.class);
        // map设置
        job.setMapperClass(HyperIndexMapper.class);
        job.setMapOutputValueClass(MapWritable.class);
        // reduce设置
        job.setReducerClass(HyperIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 输入输出设置
        job.setInputFormatClass(MongoInputFormat.class);
//        job.setOutputFormatClass(MongoOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
