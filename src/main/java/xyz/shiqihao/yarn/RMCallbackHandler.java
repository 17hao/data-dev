package xyz.shiqihao.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

public class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {

    }

    /**
     * run tasks on containers
     */
    @Override
    public void onContainersAllocated(List<Container> containers) {

    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {

    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {

    }
}
