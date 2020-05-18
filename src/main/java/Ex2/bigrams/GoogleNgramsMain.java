package Ex2.bigrams;

import Ex2.utils.NgramCountMapper;
import Ex2.utils.NgramCountReducer;
import Ex2.utils.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

import static Ex2.utils.awsVars.*;

public class GoogleNgramsMain extends Configured implements Tool {
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = getConf();
        Job job = new Job(conf, JOB_NAME);

        job.setMapperClass(NgramCountMapper.class);
        job.setReducerClass(NgramCountReducer.class);
        job.setCombinerClass(NgramCountReducer.class);

        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(LongWritable.class);

        TextInputFormat.addInputPath(job, new Path(INPUT_BUCKET_NAME+PATH));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_BUCKET_NAME+PATH));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setNumReduceTasks(1);

        job.setJarByClass(GoogleNgramsMain.class);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}
