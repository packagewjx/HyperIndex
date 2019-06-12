package io.github.packagewjx;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.Properties;

public class HyperIndexMapper extends Mapper<Object, BSONObject, Text, Text> {
    private static MongoClient mongoClient;
    private static MongoDatabase guangmingNewsDB;
    private static MongoCollection<Document> urlCollection;

    static {
        // 初始化数据库
        Properties properties = new Properties();
        try {
            properties.load(HyperIndexMapper.class.getClassLoader().getResourceAsStream("db.properties"));
            String mongoHost = properties.getProperty("mongoHost");
            mongoClient = new MongoClient(mongoHost);
            guangmingNewsDB = mongoClient.getDatabase("guangmingNews");
            urlCollection = guangmingNewsDB.getCollection("url");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private String extractUsefulText(String html) {
        StringBuilder sb = new StringBuilder();
        org.jsoup.nodes.Document doc = Jsoup.parse(html);
        // 标题
        sb.append(doc.select("title").text()).append(' ');
        // 正文
        sb.append(doc.select("p").text()).append(' ');
        // 关键字
        Elements elements = doc.select("meta[name=keywords]");
        if (elements.size() > 0) {
            sb.append(elements.get(0).attr("content")).append(' ');
        }
        // 描述
        elements = doc.select("meta[name=description]");
        if (elements.size() > 0) {
            sb.append(elements.get(0).attr("content")).append(' ');
        }
        // 作者
        elements = doc.select("meta[name=author]");
        if (elements.size() > 0) {
            sb.append(elements.get(0).attr("content")).append(' ');
        }
        return sb.toString();
    }


    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        Document document = new Document();
//        document.append("article_url", value.get("article_url"));
//        urlCollection.insertOne(document);
//        ObjectId urlId = document.getObjectId("_id");

        String html = (String) value.get("article_body_text");
        String useful = extractUsefulText(html);
        context.write(new Text("1"), new Text("1"));
    }
}
