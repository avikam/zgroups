import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WorkersConfigTest {

    @Test
    void chooseWorker() {
        {
            String worker = WorkersConfig.parseConfig("queue1:10;queue2:20").randomize().worker;
            if (!worker.equals("queue1") && !worker.equals("queue2")) {
                fail();
            }
        }

        {
            String worker = WorkersConfig.parseConfig("queue1:0;queue2:20;queue3:0;queue4:0").randomize().worker;
            assertEquals(worker, "queue2");
        }

        for (int i = 0; i < 1000; i++)
        {
            String worker = WorkersConfig.parseConfig("queue1:0;queue2:0;queue3:0;queue4:1").randomize().worker;
            assertEquals(worker, "queue4");
        }

        for (int i = 0; i < 1000; i++)
        {
            String worker = WorkersConfig.parseConfig("queue1:0;queue2:1;queue3:2;queue4:3").randomize().worker;
            assertNotEquals(worker, "queue1");
        }
    }

    @Test
    void subtractConfig() {
        WorkersConfig conf = WorkersConfig.parseConfig("queue1:3;queue2:4;queue3:2;queue4:2;queue5:5");

        {
            WorkersConfig res = conf.subtract(Map.of(
                    "queue5", 1
            ));
            assertEquals(res.find("queue5").scale, 4);
            assertEquals(res.find("queue1").scale, 3);

            // immutable
            assertEquals(conf.find("queue5").scale, 5);
        }

        {
            WorkersConfig res = conf.subtract(Map.of(
                    "queue5", 6,
                    "queue1", 3,
                    "queue2", 3
            ));
            assertEquals(res.find("queue5").scale, 0);
            assertEquals(res.find("queue1").scale, 0);
            assertEquals(res.find("queue2").scale, 1);
        }

    }
}
