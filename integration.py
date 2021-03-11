import subprocess
import signal
import time

ps = []
scale = 50
for i in range(scale):
    p = subprocess.Popen(
        ["java", "-jar", "build/libs/worker-zoo-1.0-SNAPSHOT.jar", "127.0.0.1:2181", "/example"]
    )
    ps.append(p)

while True:
    try:
        time.sleep(0.5)
    except KeyboardInterrupt:
        for p in ps:
            p.send_signal(signal.SIGINT)

        for i, p in enumerate(ps):
            print("killing", i)
            p.wait(timeout=5)

        break
