import java.util.Map;
import java.util.Random;

public class WorkersConfig {
    static final String SEP = ";";

    static class WorkerScale {
        public String worker;
        public Integer scale;
    }

    private WorkerScale[] config;

    public WorkersConfig() {}


    static public WorkersConfig parseConfig(String config) {
        int totalWeight = 0;
        String[] entries = config.split(SEP);

        WorkerScale[] ws = new WorkerScale[entries.length];
        int[] weights = new int[entries.length];

        int j = 0;
        for (String l : entries) {

            String[] workerScale = l.split(":");

            final int workerWeight = Integer.parseInt(workerScale[1]);
            WorkerScale entry = new WorkerScale() {
                {
                    worker = workerScale[0];
                    scale = workerWeight;
                }
            };

            totalWeight += workerWeight;

            weights[j] = totalWeight;
            ws[j] = entry;

            j++;
        }

        WorkersConfig result = new WorkersConfig();
        result.config = ws;
        return result;
    }

    public WorkersConfig subtract(Map<String, Integer> currWorkers) {
        WorkerScale[] cloned = config.clone();

        for (WorkerScale ws : cloned) {
            Integer currScale = currWorkers.get(ws.worker);
            // only 0 it out if no more workers are needed
            if (currScale != null && ws.scale - currScale == 0) {
                ws.scale = 0;
            }
        }

        WorkersConfig result = new WorkersConfig();
        result.config = cloned;
        return result;
    }

    public WorkerScale randomize() {
        int totalWeight = 0;
        int[] w = new int[config.length];

        int j = 0;

        for (WorkerScale ws : config) {
            System.out.print(ws.scale +  " + ");
            totalWeight += ws.scale;
            w[j++] = totalWeight;
        }

        System.out.println(" = " + config.length);
        if (totalWeight == 0) {
            return null;
        }

        Random rand = new Random();
        int randomNumber = rand.ints(1, 1, totalWeight + 1).findFirst().getAsInt();


        int lo = 0, hi = w.length - 1;
        while (lo < hi) {
            int r = lo + (hi - lo) / 2;
            if (randomNumber <= w[r]) {
                hi = r;
            } else {
                lo = r + 1;
            }
        }
        // hi <= lo

        // TODO: Rename to index
        // ws[hi].scale = weights[hi] - randomNumber;
        return config[hi];
    }

}
