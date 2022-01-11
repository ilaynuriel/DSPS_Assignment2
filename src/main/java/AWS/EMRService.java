package AWS;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class EMRService {

    private final EmrClient emr;

    public EMRService() {

        emr = EmrClient.builder()
                .region(Region.US_EAST_1)   // Region to create/load the instance
                .build();
    }

    public String runApplication(StepConfig... steps) {

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(5)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("2.6.0")
                .ec2KeyName("aws-ssh")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("3Gram-MapReduce")
                .instances(instances)
                .steps(steps)
                .logUri("s3n://" + Constants.BUCKET_NAME + "/log/")  // TODO
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-6.0.0")  // Hadoop 2.7.2
                .build();

        RunJobFlowResponse response = emr.runJobFlow(request);
        return response.jobFlowId();
    }
}
