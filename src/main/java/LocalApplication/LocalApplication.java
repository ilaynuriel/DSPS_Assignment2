package LocalApplication;

import AWS.EMRService;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.StepConfig;

public class LocalApplication {

    public static void main(String[] args) {

        awsService.StorageService s3 = new awsService.StorageService("dsps-hadoop-ilay-yuval");

        // 1. Upload jars to S3
        s3.uploadFile("target/FirstStep.jar", "FirstStep.jar");
        s3.uploadFile("target/SecondStep.jar", "SecondStep.jar");
        //s3.uploadFile("target/ThirdStep/ThirdStep.jar", "ThirdStep.jar");

        // 2. Create Config for FirstStep
        HadoopJarStepConfig hadoopJarStep1 = HadoopJarStepConfig.builder()
                .jar("s3n://dsps-hadoop-ilay-yuval/FirstStep.jar")
                .mainClass("FirstStep")
                .args("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", "s3://dsps-hadoop-ilay-yuval/output_step_1")
                .build();

        StepConfig stepConfig1 = StepConfig.builder()
                .name("FirstStep")
                .hadoopJarStep(hadoopJarStep1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        // 3. Create Config for SecondStep
        HadoopJarStepConfig hadoopJarStep2 = HadoopJarStepConfig.builder()
                .jar("s3n://dsps-hadoop-ilay-yuval/SecondStep.jar")
                .mainClass("SecondStep")
                .args("s3://dsps-hadoop-ilay-yuval/output_step_1", "s3://dsps-hadoop-ilay-yuval/output_step_2") // TODO
                .build();

        StepConfig stepConfig2 = StepConfig.builder()
                .name("SecondStep")
                .hadoopJarStep(hadoopJarStep2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

//        // 4. Create Config for ThirdStep
//        HadoopJarStepConfig hadoopJarStep3 = HadoopJarStepConfig.builder()
//                .jar("s3n://dsps-hadoop-ilay-yuval/ThirdStep.jar")
//                .mainClass("ThirdStep")
//                .args("s3://dsps-hadoop-ilay-yuval/output_step_2", "s3://dsps-hadoop-ilay-yuval/output_step_3") // TODO
//                .build();
//
//        StepConfig stepConfig3 = StepConfig.builder()
//                .name("SecondStep")
//                .hadoopJarStep(hadoopJarStep3)
//                .actionOnFailure("TERMINATE_JOB_FLOW")
//                .build();

        // 5. Run Application
        EMRService emr = new EMRService();
        String jobFlowId = emr.runApplication(stepConfig1, stepConfig2); //, stepConfigRound3);
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
