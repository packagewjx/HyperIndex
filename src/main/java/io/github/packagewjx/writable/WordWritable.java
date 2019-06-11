package io.github.packagewjx.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements Writable {
    private Text word;

    private MapWritable urlCount;

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public MapWritable getUrlCount() {
        return urlCount;
    }

    public void setUrlCount(MapWritable urlCount) {
        this.urlCount = urlCount;
    }

    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        urlCount.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        urlCount.readFields(dataInput);
    }
}
