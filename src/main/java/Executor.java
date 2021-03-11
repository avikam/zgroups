import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Executor
        implements Watcher, Runnable, DataMonitor.DataMonitorListener {
    DataMonitor dm;
    ZooKeeper zk;
    String znode;

    private static final Logger logger = LogManager.getLogger(Executor.class);


    public Executor(String hostPort, String znode) throws IOException {
        this.znode = znode;

        zk = new ZooKeeper(hostPort, 3000, this);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("USAGE: Executor hostPort znode");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = args[1];

        try {
            new Executor(hostPort, znode).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***************************************************************************
     * We do process any events ourselves, we just need to forward them on.
     *
     * org.apache.zookeeper.Watcher#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void run() {
        dm = new DataMonitor(zk, znode, null, this);
        dm.start();

        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closing(KeeperException.Code rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    public WorkersConfig onConfig(String config) {
        return WorkersConfig.parseConfig(config);
    }

    @Override
    public void converge(String worker) {
        logger.warn("Using " + worker);
    }
}
