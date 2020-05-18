package Ex2.utils;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class NgramCountReducer extends
        Reducer<TextPair, LongWritable, TextPair, LongWritable> {
    // private LongWritable result = new LongWritable();

    public void reduce(TextPair key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {

        // variable to hold the temporary sum
        long sum = 0;
        // iterator through the input to calculate the total views
        for (LongWritable longWritable : values) {
            // add the frequency to the total sum
            sum += Long.parseLong(longWritable.toString());

        }
        //writing the output
        context.write(new TextPair(key.getFirst(), key.getSecond()), new LongWritable(sum));
    }
}
