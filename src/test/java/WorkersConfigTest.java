import org.junit.jupiter.api.Test;

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
}
