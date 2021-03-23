package xyz.shiqihao.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Client <--> ResourceManager  YarnClient
 * ApplicationMaster <--> ResourceManger AMRMClient
 * ApplicationMaster <--> NodeManager NMClient
 */
public class AppYarnClient {
    public static void main(String... args) throws IOException, YarnException {
        int amMemory = 512;

        Configuration conf = new YarnConfiguration();
        YarnClient yarnClint = YarnClient.createYarnClient();
        yarnClint.init(conf);
        yarnClint.start();
        YarnClientApplication app = yarnClint.createApplication();
        // appResponse contains information about the cluster such as max/min resource capabilities of the cluster
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName("my app");

        // set local resources for the application master
        Map<String, LocalResource> localResources = new HashMap<>();

        // set the env variables to be setup in the env where application master will be run
        Map<String, String> env = new HashMap<>();

        List<String> vargs = new ArrayList<>();
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add("xyz.shiqihao.yarn.ApplicationMaster");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (String s : vargs) {
            command.append(s).append(" ");
        }

        // application master command
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        FileSystem fs = FileSystem.get(conf);

        // set up the container launch context for the application master
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

        // set up resources type requirements
        Resource capacity = Resource.newInstance(amMemory, 1);
        appContext.setResource(capacity);

        appContext.setAMContainerSpec(amContainer);

        // set the priority for the application master
        Priority priority = Priority.newInstance(1);
        appContext.setPriority(priority);

        // set the queue to which this application is to be submitted in th RM
        appContext.setQueue("default");

        yarnClint.submitApplication(appContext);
    }
}
