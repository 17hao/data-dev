package xyz.shiqihao.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

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
    public static void main(String... args) throws IOException, YarnException, InterruptedException {
        String appName = "my app";
        long amMemory = 512;
        String appJar = args[0];

        Configuration conf = new YarnConfiguration();
        conf.addResource(Thread.currentThread().getContextClassLoader().getResource("my-yarn-site.xml"));

        System.out.println("conf " + conf.getBoolean("yarn.log-aggregation-enable", false) + " "
                + conf.getInt("yarn.nodemanager.delete.debug-delay-sec", 0));
        YarnClient yarnClint = YarnClient.createYarnClient();
        yarnClint.init(conf);
        yarnClint.start();
        YarnClientApplication app = yarnClint.createApplication();
        // appResponse contains information about the cluster such as max/min resource capabilities of the cluster
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();
        amMemory = Math.min(amMemory, maxMem);

        // set the application submission context
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName(appName);

        // set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // set local resources for the application master
        Map<String, LocalResource> localResources = new HashMap<>();

        // copy application master jar from local filesystem
        //FileSystem fs = FileSystem.get(conf);
        //Path src = new Path(appJar);
        //String pathSuffix = appName + File.separator + appId.getId();
        //Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        //fs.copyFromLocalFile(src, dst);
        //FileStatus destStatus = fs.getFileStatus(dst);
        //
        //LocalResource amJar = Records.newRecord(LocalResource.class);
        //amJar.setType(LocalResourceType.FILE);
        //amJar.setVisibility(LocalResourceVisibility.APPLICATION);
        //amJar.setResource(URL.fromPath(dst));
        //amJar.setTimestamp(destStatus.getModificationTime());
        //amJar.setSize(destStatus.getLen());
        //localResources.put("my app", amJar);

        //Path log4jSrc = new Path("src/main/resources/log4j2.properties");
        //Path log4jDst = new Path(fs.getHomeDirectory(), "log4j2.properties");
        //fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
        //FileStatus log4jStatus = fs.getFileStatus(log4jDst);
        //
        //LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
        //log4jRsrc.setType(LocalResourceType.FILE);
        //log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        //log4jRsrc.setResource(URL.fromPath(log4jDst));
        //log4jRsrc.setTimestamp(log4jStatus.getModificationTime());
        //log4jRsrc.setSize(log4jStatus.getLen());
        //localResources.put("log4j2.properties", log4jRsrc);

        amContainer.setLocalResources(localResources);

        // set the env variables to be setup in the env where application master will be run
        Map<String, String> env = new HashMap<>();
        StringBuilder classPathEnv = new StringBuilder().append(appJar);
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put(ApplicationConstants.Environment.CLASSPATH.name(), classPathEnv.toString());
        amContainer.setEnvironment(env);

        System.out.println("CLASSPATH: " + System.getenv("CLASSPATH"));

        List<String> vargs = new ArrayList<>();
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-classpath " + classPathEnv.toString());
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add("xyz.shiqihao.yarn.ApplicationMaster");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (String s : vargs) {
            command.append(s).append(" ");
        }
        System.out.println("AM startup command: " + command.toString());

        // application master command
        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        amContainer.setCommands(commands);

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

        ApplicationReport appReport = yarnClint.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.FAILED &&
                appState != YarnApplicationState.KILLED) {
            Thread.sleep(500);
            appReport = yarnClint.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();

            System.out.println("Application: " + appId + "(" + appState + ") at: " + appReport.getFinishTime());
        }

        yarnClint.stop();
        yarnClint.close();
    }
}
