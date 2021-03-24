package xyz.shiqihao.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;

public class ApplicationMaster {
    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        int containerNum = 2;
        Configuration conf = new YarnConfiguration();
        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // register with resource manager
        rmClient.registerApplicationMaster("localhost", 0, "");

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(256);
        capability.setVirtualCores(1);

        // make container request to resource manager
        for (int i = 0; i < containerNum; i++) {
            AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(capability, null, null, priority);
            rmClient.addContainerRequest(containerRequest);
        }

        // obtain allocated containers and launch
        int allocatedContainers = 0;
        int completedContainers = 0;
        while (allocatedContainers < containerNum) {
            AllocateResponse response = rmClient.allocate(0);
            for (Container c : response.getAllocatedContainers()) {
                allocatedContainers++;

                // launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList("java -jar /Users/17hao/springboot.jar"));

                nmClient.startContainer(c, ctx);
            }

            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                completedContainers++;
                System.out.println("Completed container: " + status);
            }
            Thread.sleep(100);
        }

        // wait for the remaining containers to complete
        while (completedContainers < containerNum) {
            AllocateResponse response = rmClient.allocate(completedContainers / containerNum);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                completedContainers++;
                System.out.println("Completed container: " + status);
            }
        }

        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");

        System.out.println("hello, world");
    }
}
