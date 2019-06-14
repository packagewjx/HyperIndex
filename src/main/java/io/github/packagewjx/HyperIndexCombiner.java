package io.github.packagewjx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

public class HyperIndexCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
    private static Logger logger = Logger.getLogger("stdout");

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        logger.info(String.format("Combining %s", key.toString()));
        MapWritable count = new MapWritable();
        values.forEach(wc -> {
            Set<Writable> urlIds = wc.keySet();
            urlIds.forEach(urlId -> {
                IntWritable urlCount = ((IntWritable) count.getOrDefault(urlId, new IntWritable(0)));
                IntWritable wcUrlCount = (IntWritable) wc.get(urlId);
                count.put(urlId, new IntWritable(urlCount.get() + wcUrlCount.get()));
            });
        });

        context.write(key, count);
        logger.info(String.format("Combine %s complete", key.toString()));
    }
}
