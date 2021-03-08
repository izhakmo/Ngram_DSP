import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;


public class MainRunner {
    public static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
    public static AmazonElasticMapReduce emr;

    public static void main(String[] args) {

        System.out.println("Creating EMR instance");
        System.out.println("===========================================");
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();


		/*
        Step No 1: Get N and split corpus
		 */
        String jar_path = null;
        String output_path = null;
        String combiner_or_not = null;
        String log_path = null;
		try {
            jar_path = args[0];
            output_path = args[1];
            combiner_or_not = (args.length > 2 && args[2].equals("yes")) ? "yes" : "no";
            log_path = output_path.replaceAll("s3","s3n");
            log_path = log_path.concat("logs/");
        }
		catch (Exception e){
		    System.out.println("please enter 3 args: jar_path, output_path");
		    System.out.println("3.enter or no if you want combiner");
		    System.exit(1);
        }
        HadoopJarStepConfig only_step = new HadoopJarStepConfig()
                .withJar(jar_path)
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data", output_path, combiner_or_not);
        StepConfig onlyStep = new StepConfig()
                .withName("all_steps")
                .withHadoopJarStep(only_step)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


		/*
        Set instances
		 */

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("omer_and_tzuki")
                .withPlacement(new PlacementType("us-east-1a"))
                .withKeepJobFlowAliveWhenNoSteps(false);

		/*
        Run all jobs
		 */
        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("omer_and_tzuki")
                .withInstances(instances)
                .withSteps(onlyStep)
                .withLogUri(log_path)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");
        RunJobFlowResult result = emr.runJobFlow(request);
        String id = result.getJobFlowId();
        System.out.println("Id: " + id);
    }
}