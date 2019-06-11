package io.github.packagewjx.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GuangMingWangWritable implements Writable {
    private Text url;
    private Text html;

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public Text getHtml() {
        return html;
    }

    public void setHtml(Text html) {
        this.html = html;
    }

    public void write(DataOutput dataOutput) throws IOException {
        url.write(dataOutput);
        html.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        url.readFields(dataInput);
        html.readFields(dataInput);
    }
}
