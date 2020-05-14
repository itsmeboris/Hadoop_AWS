package Ex2.bigrams;
import Ex2.bigrams.Pairs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class main {
    public static void main(String[] args) {

       /* AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonElasticMapReduce mapReduce =  AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(String.valueOf(REGION))
                .build();
        HadoopJarStepConfig hadoopJarStepConfig= awsVars.HadoopJarStepsConfig();
        StepConfig stepConfig = new StepConfig()
                .withName(STEP_NAME)
                .withHadoopJarStep(hadoopJarStepConfig)
                .withActionOnFailure(ACTION_ON_FAILURE);
        JobFlowInstancesConfig jobFlowInstancesConfig = new JobFlowInstancesConfig()
                .withInstanceCount(MAX_INSTANCES)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion(HADOOP_VERSION)
                .withEc2KeyName(KEY_PAIR_NAME)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(REGION.toString()));
        RunJobFlowRequest runJobFlowRequest = new RunJobFlowRequest()
                .withName(JOB_NAME)
                .withInstances(jobFlowInstancesConfig)
                .withSteps(stepConfig)
                .withLogUri(getS3Address(LOG_BUCKET_NAME));
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runJobFlowRequest);
        String jobFlowID = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowID);
        */



       //        %HADOOP_HOME%/bin/hadoop jar target/Ex2-1.0-SNAPSHOT-jar-with-dependencies.jar

        try {
           int res = ToolRunner.run(new Configuration(), new Pairs(), args);
           System.exit(res);
       } catch (Exception e) {
           e.printStackTrace();
           System.exit(-1);
       }
    }
}
