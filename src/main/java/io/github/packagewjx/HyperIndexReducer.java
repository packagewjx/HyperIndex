package io.github.packagewjx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HyperIndexReducer extends Reducer<Text, MapWritable, Text, Document> {
    private static Logger logger = Logger.getLogger("stdout");

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        logger.log(Level.INFO, String.format("Reducing word %s", key.toString()));

        Map<String, Integer> count = new HashMap<>();
        values.forEach(wc -> {
            Set<Writable> urlIds = wc.keySet();
            urlIds.forEach(urlId -> {
                Integer urlCount = count.getOrDefault(urlId.toString(), 0);
                IntWritable wcUrlCount = (IntWritable) wc.get(urlId);
                count.put(urlId.toString(), urlCount + wcUrlCount.get());
            });
        });
        Document doc = new Document();
        doc.append("_id", key.toString());
        doc.append("word", key.toString());
        doc.append("count", count);
        context.write(key, doc);
        logger.info(String.format("Reduce word %s complete", key.toString()));
    }
}
