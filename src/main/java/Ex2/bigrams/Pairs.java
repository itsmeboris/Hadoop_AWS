package Ex2.bigrams;

import Ex2.utils.TextPair;
import Ex2.utils.awsVars;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import static Ex2.utils.awsVars.*;

public class Pairs extends Configured implements Tool {

    public final static IntWritable ONE = new IntWritable(1);

    private Path inputPath = new Path(INPUT_BUCKET_NAME+PATH);
    private Path outputPath = new Path(OUTPUT_BUCKET_NAME+PATH);

    public static class PairsMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

        private TextPair pair;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            pair = new TextPair();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            String[] tokens = filterStopWords(value.toString(), ENG);

            for (int i = 0; i < tokens.length-1; i++) {
                pair.set(tokens[i], tokens[i+1]);
                context.write(pair, ONE);
            }
        }
    }

    public static class PairsReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

        private IntWritable res;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            res = new IntWritable();
        }

        @Override
        public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            res.set(sum);
            context.write(key, res);
        }
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, JOB_NAME);

        job.setMapperClass(PairsMapper.class);
        job.setReducerClass(PairsReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(NUMBER_REDUCERS);

        job.setJarByClass(Pairs.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
