package Ex2.bigrams;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static Ex2.utils.awsVars.*;

/*
    java -cp /target/Ex2-1.0-SNAPSHOT-jar-with-dependencies.jar Ex2.bigrams.FileReader s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data /user/output
       <property>
      <name>fs.s3n.awsAccessKeyId</name>
      <value>AWS-ID</value>
   </property>
   <property>
      <name>fs.s3n.awsSecretAccessKey</name>
      <value>AWS-SECRET-KEY</value>
   </property>
 */
public class FileReader extends Configured implements Tool{
    // Map function
    public static class SFMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println(value.toString());
            context.write(key, value);
        }
    }
    public static void main(String[] args)  throws Exception{
        int flag = ToolRunner.run(new FileReader(), args);
        System.exit(flag);

    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "seqfileread");
        job.setJarByClass(FileReader.class);

        job.setMapperClass(SFMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_BUCKET_NAME+PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_BUCKET_NAME));

        int returnFlag = job.waitForCompletion(true) ? 0 : 1;
        return returnFlag;
    }
}