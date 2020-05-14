package Ex2.utils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static Ex2.utils.awsVars.*;


public class AWS {
    private Ec2Client ec2;
    private S3Client s3;
    private Region region = REGION;

    public AWS(){
        initEC2();
        initS3();
    }

    public void initEC2(){
        ec2 = Ec2Client.builder()
                .region(region)
                .build();
    }

    public void initS3(){
        s3 = S3Client.builder()
                .region(region)
                .build();
    }

    public ArrayList<String> EC2initiateInstance(String instanceImageID, int min, int max, String instanceType, String userData, Tag tag) {
        String script = null;
        try {
            script = getScript(userData);
            System.out.println("Script: " + Arrays.toString(Base64.getDecoder().decode(script)));
        } catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(instanceImageID)
                .instanceType(instanceType)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .name(IAM_PROFILE_NAME)
                        .build())
                .maxCount(max)
                .minCount(min)
                .keyName(KEY_PAIR_NAME)
                .securityGroups(SECURITY_GROUP)
                .userData(script)
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        ArrayList<String> idAllInstances = new ArrayList<>();
        for (Instance instance : instances){
            try {
                String instanceID = instance.instanceId();
                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceID)
                        .tags(tag)
                        .build();
                ec2.createTags(tagRequest);
                idAllInstances.add(instanceID);
            } catch (Ec2Exception e){
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        return idAllInstances;
    }

    private String getScript(String fileName) throws IOException {
        String ans = readFile(fileName);
        return Base64.getEncoder()
                .encodeToString(
                        ans.getBytes());
    }

    public String readFile(String fileName) throws IOException {
        StringBuilder ans = new StringBuilder();
        File file = new File(fileName);
        Scanner sc = new Scanner(file);
        while(sc.hasNextLine()) {
            ans.append(sc.nextLine());
            ans.append(System.lineSeparator());
        }
        sc.close();
        return ans.toString();
    }

    public ResponseInputStream<GetObjectResponse> S3getObject(String bucketName) {
        return s3.getObject(GetObjectRequest.builder().bucket(bucketName).build(),ResponseTransformer.toInputStream());
    }

    public String EC2SearchByTag(Tag tag, String state) {
        DescribeInstancesResponse response = ec2.describeInstances();
        List<Reservation> reservations = response.reservations();

        Set<Instance> instances = new HashSet<>();
        for(Reservation reservation: reservations){
            instances.addAll(reservation.instances());
        }

        for(Instance instance: instances){
            InstanceState instanceState = instance.state();
            List<Tag> isTags = instance.tags();
            if(isTags.contains(tag)){
                if(instanceState.nameAsString().equals(state)){
                    return instance.instanceId();
                }
            }
        }
        return null;
    }

    public String S3UploadFile(String bucketName, String key, File file) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(CreateBucketConfiguration.builder()
                        .build())
                .build();
        s3.createBucket(createBucketRequest);
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .acl(ObjectCannedACL.PUBLIC_READ)
                .build(),RequestBody.fromFile(file));
        return "https://s3.amazonaws.com/"+bucketName+"/"+key;
    }

        public void S3DownloadFiles(String bucketName, String key, File file) {
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(),
                ResponseTransformer.toFile(file));
    }

    public void EC2TerminateInstance(String instanceID) {
        ec2.terminateInstances(TerminateInstancesRequest.builder()
                .instanceIds(instanceID)
                .build());
    }

    public void EC2TerminateInstance(Collection<String> instanceIDs) {
        ec2.terminateInstances(TerminateInstancesRequest.builder()
                .instanceIds(instanceIDs)
                .build());
    }

    public void deleteAllBuckets() {
        List<Bucket> buckets = s3.listBuckets().buckets();
        for(Bucket b: buckets){
            if(!b.name().equals(APPLICATION_CODE_BUCKET_NAME)) {
                List<S3Object> s3ObjectList = s3.listObjects(ListObjectsRequest.builder()
                        .bucket(b.name())
                        .build()).contents();
                for (S3Object s3Object : s3ObjectList) {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(b.name())
                            .key(s3Object.key())
                            .build());
                }
                s3.deleteBucket(DeleteBucketRequest.builder()
                        .bucket(b.name())
                        .build());
            }
        }
    }

    public void InitAllServices() {
        initEC2();
        initS3();
    }
}