package Ex2.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static Ex2.utils.awsVars.checkStopWords;

public class NgramCountMapper extends
        Mapper<LongWritable, Text, TextPair, LongWritable> {
    private TextPair gram = new TextPair();
    private LongWritable frequency = new LongWritable();
    private final static String ASTERISK = "*";

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String word = value.toString().toLowerCase();
        int tempInt = -1;
        String[] parts = word.split("\\s+");
        if(!checkStopWords(parts)) {
            if ((parts.length == 4) && (parts[0].matches("[a-z]+"))) {
                // writing as output
                gram.set(parts[0].trim() + " " + ASTERISK, parts[1]);
                long tempFreq = Long.parseLong(parts[2]);
                frequency.set(tempFreq);
            } else if ((parts.length == 5) && (parts[0].matches("[a-z]+")) && (parts[1].matches("[a-z]+"))) {
                // writing as output
                gram.set(parts[0].trim() + " " + parts[1], parts[2]);
                long tempFreq = Long.parseLong(parts[3]);
                frequency.set(tempFreq);
            }
            context.write(gram, frequency);
        }
    }
}

