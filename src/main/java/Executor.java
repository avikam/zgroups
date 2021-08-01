import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


public class Executor
        implements Program, Watcher, Runnable, DataMonitor.DataMonitorListener {

    DataMonitor dm;
    ZooKeeper zk;
    String znode;
    ProcessBuilder processBuilder;
    List<Process> processes;

    private static final Logger logger = LogManager.getLogger(Executor.class);


    public Executor(String hostPort, String znode, String[] args) throws IOException {
        this.znode = znode;
        this.processes = new LinkedList<>();

        if (args.length > 2) {
            processBuilder = new ProcessBuilder().command(args);
        }

        zk = new ZooKeeper(hostPort, 3000, this);
        dm = new DataMonitor(zk, znode, null, this);
    }

    public void main() {
        try {
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownHandler));

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
    public void converge(String group) {
        logger.debug("Using {}", group);
        if (processBuilder == null) {
            logger.warn("Using {} - No process is executed", group);
            return;
        }

        ProcessBuilder pb = processBuilder
                .command(
                        processBuilder.command().stream().map(s -> s.equals("{}") ? group : s).collect(Collectors.toList())
                ).inheritIO();

        Map<String, String> env = pb.environment();
        env.put("ZGROUPS_GROUP", group);

        try {
            Process p = pb.start();
            logger.info("command: {} => pid: {}", pb.command(), p.pid());
            processes.add(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void shutdownHandler() {
        logger.info("interrupted, ending zookeeper session");
        dm.stop();
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Clear zombies?
        for (Process p : processes) {
            logger.debug("Killing & Cleaning process {}", p.pid());
            try {
                p.destroy();
                if (!p.waitFor(5, TimeUnit.SECONDS)) {
                    logger.warn("Forcibly killing {}", p.pid());
                    p.destroyForcibly();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
