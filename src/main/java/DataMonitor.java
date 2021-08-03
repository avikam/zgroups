import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.apache.zookeeper.ZooDefs.Ids.ANYONE_ID_UNSAFE;

public class DataMonitor {
    // zookeeper handle
    ZooKeeper zk;

    Watcher chainedWatcher;

    private boolean stopped;
    DataMonitorListener listener;

    // The current distribution of nodes. Read from the ephemeral children of configNodePath
    Map<String, Integer> currentClusterConfig;
    List<String> currChildren;
    // The desired distribution of nodes. Read form the content of configNodePath.
    WorkersConfig nodeConfig;

    // The path this instance created specifying its allocation
    private String currentPath = "";
    WorkersConfig.WorkerScale candidateWorker = null;

    // ZooKeeper path that holds the group configuration for this cluster
    final private String configNodePath;

    // Object to find the configNodePath and read it.
    final private ConfigurationNodeFinder confFinder;
    // Object that lists the children under the configNodePath
    final private ClusterCurrentConfiguration clusterConfFinder;

    private static final Logger logger = LogManager.getLogger(DataMonitor.class);

    private final List<ACL> unsafeAcl = new ArrayList<>() {
        {
            add(new ACL(ZooDefs.Perms.ALL, ANYONE_ID_UNSAFE));
        }
    };
    private boolean processConverged = false;


    public DataMonitor(ZooKeeper zk, String configNode, Watcher chainedWatcher,
                       DataMonitorListener listener) {

        this.zk = zk;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        this.stopped = false;
        this.configNodePath = configNode;

        this.confFinder = new ConfigurationNodeFinder(this, zk, configNode);
        this.clusterConfFinder = new ClusterCurrentConfiguration(this, zk, configNode);
    }

    public void start() {
        confFinder.Find();
    }

    private void restart() {
        confFinder.Find();
    }

    public void stop() {
        stopped = true;
    }

    public boolean stopped() {
        return stopped;
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface DataMonitorListener {
        void closing(KeeperException.Code rc);
        void converge(String worker);
    }

    private void zkStopping(KeeperException.Code rc) {
        stopped = true;
        listener.closing(rc);
    }

    private Map<String, Integer> currentClusterToConfig(List<String> children) {
        Map<String, Integer> map = new HashMap<>();
        for (String child : children) {
            logger.debug("found child: " + child);

            String[] parts = child.split(SEP);
            if (parts.length != 2) {
                throw new RuntimeException("expected exactly 2 parts in a name of a child: " + child);
            }

            String workerName = parts[0];
            Integer val = map.get(workerName);
            if (val == null) {
                val = 0;
            }

            map.put(workerName, val + 1);
        }

        return map;
    }

    private boolean converged() {
        return processConverged;
    }

    private void selectGroup() throws KeeperException, InterruptedException {
        if (converged()) {
            return;
        }

        logger.info("effective config {}", currentClusterConfig);
        logger.info("desired config {}", nodeConfig);

        // Create ephemeral+counter node and make sure we did not exceed the amount of workers
        if (currentPath.isEmpty()) {
            WorkersConfig avail = nodeConfig.subtract(currentClusterConfig);
            logger.info("choosing from {}", avail);

            candidateWorker = avail.randomize();
            if (candidateWorker == null) {
                logger.warn("No room for me. pending for updates");
                clusterConfFinder.Find();

                return;
            }

            String createdPath = zk.create(
                    configNodePath + "/" + entryName(candidateWorker.worker),
                    null,
                    unsafeAcl,
                    CreateMode.EPHEMERAL_SEQUENTIAL
            );
            currentPath = createdPath;

            logger.info("Candidate " + currentPath + ", Relisting children to validate this choice");
            clusterConfFinder.Find();

            return;
        } else {
            String createNode = currentPath.substring(configNodePath.length() + 1);

            // we could be called because of an old watcher.
            // this will set to true if the candidate node is listen within the children
            boolean isSync = false;

            // Make sure the workers "behind" me are not more than my index.
            List<String> current = currChildren; // has to be a refreshed list
            int smaller = 0;
            for (String curr : current) {
                if (curr.equals(createNode)) {
                    isSync = true;
                }

                logger.debug("curr: " + curr);

                if (curr.startsWith(candidateWorker.worker)) {
                    smaller += (createNode.compareTo(curr) > 0) ? 1 : 0;
                }
            }

            if (!isSync) {
                logger.info("candidate node is not listed. Waiting for another call");
                return;
            }

            Integer maxAllowed = nodeConfig.find(candidateWorker.worker).scale;

            logger.info("Created " + createNode + ", there are " + smaller +
                    " before me while there could be, including me, at most" + maxAllowed
            );

            if (smaller >= maxAllowed) {
                logger.warn("Too crowded, leaving");
                zk.delete(currentPath, -1);

                currentPath = "";
                candidateWorker = null;
                return;
            }
        }

        processConverged = true;
        listener.converge(candidateWorker.worker);
    }

    private void OnNodeConfig(String config) {
        nodeConfig = WorkersConfig.parseConfig(config);
        logger.info("configuration read {}", nodeConfig);
        clusterConfFinder.Find();
    }

    private void OnClusterConfig(List<String> config) {
        currentClusterConfig = currentClusterToConfig(config);
        currChildren = config;
        logger.info("set up current config {}", currentClusterConfig);
        onConfig();
    }

    private void onConfig() {
        if (nodeConfig != null && currentClusterConfig != null) {
            try {
                selectGroup();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("got desired, missing current; or got current missing desired");
            logger.info("waiting...");
        }
    }

    final private static String SEP = "_";

    static String entryName(String name) {
        // Make sure SEP not in name
        return name + SEP;
    }

    static class ClusterCurrentConfiguration implements Watcher, AsyncCallback.ChildrenCallback {
        final DataMonitor parent;
        final ZooKeeper zk;
        final String configNode;

        private Boolean stopped;

        ClusterCurrentConfiguration(DataMonitor parent, ZooKeeper zk, String configNode) {
            this.parent = parent;
            this.zk = zk;
            this.configNode = configNode;
        }

        public void Find() {
            find();
        }

        private void find() {
            if (parent.converged()) {
                return;
            }

            zk.getChildren(configNode, parent.stopped ? null : this, this, null);
        }

        public void process(WatchedEvent event) {
            String path = event.getPath();
            if (event.getType() == Watcher.Event.EventType.None) {
                // The state of the connection has changed
                switch (event.getState()) {
                    case SyncConnected:
                        // In this particular example we don't need to do anything
                        // here - watches are automatically re-registered with
                        // server and any watches triggered while the client was
                        // disconnected will be delivered (in order of course)
                        break;
                    case Expired:
                        // It's all over
                        parent.zkStopping(KeeperException.Code.SESSIONEXPIRED);
                        break;
                }
            } else {
                if (path != null && path.equals(configNode)) {
                    // Something has changed on the node, let's find out
                    switch (event.getType()) {
                        case NodeDataChanged -> parent.restart();
                        case NodeChildrenChanged -> find();
                    }
                }
            }
//            if (chainedWatcher != null) {
//                chainedWatcher.process(event);
//            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            parent.OnClusterConfig(children);
        }
    }

    static class ConfigurationNodeFinder implements StatCallback {
        final DataMonitor parent;
        final ZooKeeper zk;
        final String configNode;

        private String latestConfigValue;


        ConfigurationNodeFinder(DataMonitor dm, ZooKeeper zk, String configNode) {
            this.parent = dm;
            this.zk = zk;
            this.configNode = configNode;

            this.latestConfigValue = "";
        }

        public void Find() {
            zk.exists(configNode, !parent.stopped, this, null);
        }

        public void processResult(int rc_, String path, Object ctx, Stat stat) {
            boolean exists;
            KeeperException.Code rc = KeeperException.Code.get(rc_);

            // Retry errors
            switch (rc) {
                case OK -> exists = true;
                case NONODE -> exists = false;
                case SESSIONEXPIRED, NOAUTH -> {
                    parent.zkStopping(rc);
                    return;
                }
                default -> {
                    zk.exists(configNode, !parent.stopped, this, null);
                    return;
                }
            }

            String config = "";
            if (exists) {
                try {
                    byte[] b = zk.getData(configNode, true, null);
                    config = new String(b, StandardCharsets.UTF_8);

                } catch (KeeperException e) {
                    // We don't need to worry about recovering now. The watch
                    // callbacks will kick off any exception handling
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    return;
                }
            }

            if (!config.equals(latestConfigValue)) {
                latestConfigValue = config;
                parent.OnNodeConfig(latestConfigValue);
            }
        }
    }
}
