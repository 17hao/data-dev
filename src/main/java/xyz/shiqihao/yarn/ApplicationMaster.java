package xyz.shiqihao.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;


public class ApplicationMaster {
    private final Configuration conf;

    private final AMRMClientAsync amRMClient;
    private final NMClientAsync nmClient;
    private final AMRMClientAsync.AbstractCallbackHandler allocatorHandler;
    private final NMClientAsync.AbstractCallbackHandler containerListener;

    private final String appHostName = "aliyun";
    private final int numTotalContainers = 2;
    private final int containerMemory = 512;
    private final String appJar = "~/xxx.jar";
    private final int priority = 0;

    public ApplicationMaster() {
        conf = new YarnConfiguration();
        allocatorHandler = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocatorHandler);
        containerListener = new NMCallbackHandler();
        nmClient = NMClientAsync.createNMClientAsync(containerListener);
    }

    public static void main(String[] args) {
    }

    private void run() throws IOException, YarnException {
        amRMClient.init(conf);
        amRMClient.start();

        nmClient.init(conf);
        nmClient.start();

        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster("", 9999, "google.com");

        long maxMem = response.getMaximumResourceCapability().getMemorySize();

        for (int i = 0; i < numTotalContainers; i++) {
            AMRMClient.ContainerRequest containerRequest = AMRMClient.ContainerRequest.newBuilder().build();
            amRMClient.addContainerRequest(containerRequest);
        }
    }

    private class LaunchContainerRunnable implements Runnable {
        Container container;
        NMCallbackHandler containerListener;

        LaunchContainerRunnable(Container container, NMCallbackHandler containerListener) {
            this.container = container;
            this.containerListener = containerListener;
        }

        @Override
        public void run() {
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            //containerListener.container
            nmClient.startContainerAsync(container, ctx);
        }
    }
}
