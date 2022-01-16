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
                .instanceCount(8)
                .masterInstanceType(InstanceType.M4_XLARGE.toString())
                .slaveInstanceType(InstanceType.M4_XLARGE.toString())
                .hadoopVersion("3.2.1")
                .ec2KeyName("vockey")
                .keepJobFlowAliveWhenNoSteps(false)
                .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("3Gram-MapReduce")
                .instances(instances)
                .steps(steps)
                .logUri("s3n://dsps-hadoop-ilay-yuval/log/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-6.4.0")  // Hadoop 3.2.1
                .build();

        RunJobFlowResponse response = emr.runJobFlow(request);
        return response.jobFlowId();
    }
}
