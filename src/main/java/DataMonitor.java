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

public class DataMonitor implements Watcher, StatCallback {

    ZooKeeper zk;
    String configNode;
    Watcher chainedWatcher;
    boolean dead;
    DataMonitorListener listener;

    Map<String, Integer> currentConfig;
    private String desiredConfig = "";

    private boolean converged;

    private static final Logger logger = LogManager.getLogger(DataMonitor.class);


    private final List<ACL> unsafeAcl = new ArrayList<>() {
        {
            add(new ACL(ZooDefs.Perms.ALL, ANYONE_ID_UNSAFE));
        }
    };


    public DataMonitor(ZooKeeper zk, String znode, Watcher chainedWatcher,
                       DataMonitorListener listener) {

        this.zk = zk;
        this.configNode = znode;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
    }

    public void start() {
        // Check for existence of the configure node
        zk.exists(configNode, true, this, null);
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface DataMonitorListener {
        /**
         * The ZooKeeper session is no longer valid.
         */
        void closing(KeeperException.Code rc);

        // Choose a worker
        WorkersConfig onConfig(String config);

        void converge(String worker);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    listener.closing(KeeperException.Code.SESSIONEXPIRED);
                    break;
            }
        } else {
            if (path != null && path.equals(configNode)) {
                // Something has changed on the node, let's find out
                switch (event.getType()) {
                    case NodeDataChanged -> zk.exists(configNode, true, this, null);

                    case NodeChildrenChanged -> currentConfig();
                }
            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

    // stats callback
    public void processResult(int rc_, String path, Object ctx, Stat stat) {
        boolean exists;
        KeeperException.Code rc = KeeperException.Code.get(rc_);

        // Retry errors
        switch (rc) {
            case OK -> exists = true;
            case NONODE -> exists = false;
            case SESSIONEXPIRED, NOAUTH -> {
                dead = true;
                listener.closing(rc);
                return;
            }
            default -> {
                zk.exists(configNode, true, this, null);
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

        if (!config.equals(desiredConfig)) {
            desiredConfig = config;
            try {
                onConfig();
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void currentConfig() {
        List<String> children;
        try {
            children = zk.getChildren(configNode, true);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return;
        }

        Map<String, Integer> map = new HashMap<>();

        for (String child : children) {
            logger.debug("curr: " + child);

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

        if (map != null) {
            currentConfig = map;
            logger.info("set up current config {}", currentConfig);
        }

        try {
            onConfig();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void onConfig() throws KeeperException, InterruptedException {
        if (converged) {
            return;
        }

        WorkersConfig parsedConfig = listener.onConfig(desiredConfig);

        if (currentConfig != null) {
            logger.info("effective config {}", currentConfig);
            parsedConfig.subtract(currentConfig);
        }


        WorkersConfig.WorkerScale worker = parsedConfig.randomize();
        if (worker == null) {
            logger.warn("No room for me. leaving");

            zk.exists(configNode, true);
            zk.getChildren(configNode, true);

            return;
        }

        // Create ephemeral+counter node and make sure we did not exceed the amount of workers
        String createdPath = zk.create(
                this.configNode + "/" + entryName(worker.worker),
                null, unsafeAcl, CreateMode.EPHEMERAL_SEQUENTIAL
        );

        String createNode = createdPath.substring(configNode.length() + 1);

        // Make sure the workers "behind" me are not more than my index.
        List<String> current = zk.getChildren(this.configNode, true);
        int smaller = 0;
        for (String curr : current) {
            logger.debug("curr: " + curr);

            if (curr.startsWith(worker.worker)) {
                smaller += (createNode.compareTo(curr) > 0) ? 1 : 0;
            }
        }

        logger.info("Created " + createNode + ", there are " + smaller +
                " before me while there should be " + worker.scale
        );

        if (smaller >= worker.scale) {
            logger.warn("Too crowded, leaving");
            zk.delete(createdPath, -1);

            zk.exists(configNode, true);
            zk.getChildren(configNode, true);

            return;
        }

        converged = true;
        listener.converge(worker.worker);
    }

    final static String SEP = "_";

    static String entryName(String name) {
        // Make sure SEP not in name
        return name + SEP;
    }
}
