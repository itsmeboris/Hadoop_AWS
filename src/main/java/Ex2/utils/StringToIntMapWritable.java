package Ex2.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class StringToIntMapWritable implements Writable {

    private HashMap<String, Integer> hm = new HashMap<>();

    public void clear() {
        hm.clear();
    }

    public String toString() {
        return hm.toString();
    }

    public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        hm.clear();
        for (int i=0; i<len; i++) {
            int l = in.readInt();
            byte[] ba = new byte[l];
            in.readFully(ba);
            String key = new String(ba);
            Integer value = in.readInt();
            hm.put(key, value);
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(hm.size());

        for (Entry<String, Integer> pairs : hm.entrySet()) {
            String k = pairs.getKey();
            Integer v = pairs.getValue();
            out.writeInt(k.length());
            out.writeBytes(k);
            out.writeInt(v);
        }
    }

    public void increment(String t) {
        int count = 1;
        if (hm.containsKey(t)) {
            count = hm.get(t) + count;
        }
        hm.put(t, count);
    }

    public void increment(String t, int value) {
        int count = value;
        if (hm.containsKey(t)) {
            count = hm.get(t) + count;
        }
        hm.put(t, count);
    }

    public void sum(StringToIntMapWritable h) {
        for (Entry<String, Integer> pairs : h.hm.entrySet()) {
            String k = pairs.getKey();
            Integer v = pairs.getValue();
            increment(k, v);
        }
    }
}
