package Ex2.utils;

import org.apache.hadoop.io.MapWritable;

public class MyMapWriteable extends MapWritable {

    public String toString() {
        return this.values().toString();
    }

    public int length() {
        return this.values().size();
    }
}
