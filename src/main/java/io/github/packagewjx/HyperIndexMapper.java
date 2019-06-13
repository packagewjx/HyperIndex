package io.github.packagewjx;

import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class HyperIndexMapper extends Mapper<Object, BSONObject, Text, MapWritable> {
    private static Logger logger = Logger.getLogger("stdout");
    private static MongoClient mongoClient;
    private static MongoDatabase guangmingNewsDB;
    private static MongoCollection<Document> urlCollection;

    static {
        // 初始化数据库
        Properties properties = new Properties();
        try {
            InputStream fin = HyperIndexMapper.class.getClassLoader().getResourceAsStream("db.properties");
            if (fin == null) {
                System.out.println("no db.properties");
                System.exit(1);
            }
            properties.load(fin);
            String mongoHost = properties.getProperty("mongoHost");
            fin.close();
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
        // 作者
        Elements elements = doc.select("meta[name=author]");
        if (elements.size() > 0) {
            sb.append(elements.get(0).attr("content")).append(' ');
        }
        return sb.toString();
    }

    private List<String> segmentAndFilter(String text) {
        List<Term> segment = IndexTokenizer.segment(text);
        return segment.stream()
                .filter(term -> term.nature != Nature.w
                        && term.nature != Nature.c
                        && term.nature != Nature.p
                        && term.nature != Nature.uj
                        && term.nature != Nature.ul
                        && term.nature != Nature.ud
                        && term.nature != Nature.r)
                .map(term -> term.word)
                .collect(Collectors.toList());
    }


    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        String articleUrl = ((String) value.get("article_url"));
        logger.log(Level.INFO, String.format("Mapping URL: %s", articleUrl));
        Document document = new Document();
        document.append("article_url", articleUrl);
        logger.log(Level.INFO, String.format("Inserting URL %s into url collection", articleUrl));
        urlCollection.insertOne(document);
        ObjectId urlId = document.getObjectId("_id");
        logger.log(Level.INFO, String.format("Inserted URL %s into url collection, get _id: %s", articleUrl, urlId.toHexString()));

        String html = (String) value.get("article_body_text");
        String useful = extractUsefulText(html);
        List<String> words = segmentAndFilter(useful);
        logger.log(Level.INFO, String.format("Extracted %d words from %s", words.size(), articleUrl));

        words.forEach(word -> {
            Text wordText = new Text(word);
            MapWritable map = new MapWritable();
            map.put(new Text(urlId.toHexString()), new IntWritable(1));
            try {
                context.write(wordText, map);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        logger.info(String.format("Map URL %s complete", articleUrl));
    }
}
