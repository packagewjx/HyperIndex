package io.github.packagewjx;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;

public class HyperIndexReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        MapWritable count = new MapWritable();
        values.forEach(wc -> {
            Set<Writable> urlIds = wc.keySet();
            urlIds.forEach(urlId -> {
                IntWritable urlCount = (IntWritable) count.getOrDefault(urlId, new IntWritable(0));
                IntWritable wcUrlCount = (IntWritable) wc.get(urlId);
                count.put(urlId, new IntWritable(urlCount.get() + wcUrlCount.get()));
            });
        });
        context.write(key, count);
    }
}
