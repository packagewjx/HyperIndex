package io.github.packagewjx.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class URLWritable implements Writable {
    private Text url;

    private IntWritable id;

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public IntWritable getId() {
        return id;
    }

    public void setId(IntWritable id) {
        this.id = id;
    }

    public void write(DataOutput dataOutput) throws IOException {
        id.write(dataOutput);
        url.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id.readFields(dataInput);
        url.readFields(dataInput);
    }
}
