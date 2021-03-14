import os
import subprocess
import signal
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import List, Tuple

import pytest

# ZK_CONNECTION_STRING - Optional. If not specified, the test will start a zookeeper server
ZK_CONNECTION_STRING = os.environ.get('ZK_CONNECTION_STRING')

# ZK_HOME_DIR - Optional. The directory of the binary installation of zookeeper.
ZK_HOME_DIR = Path(os.environ.get('ZK_HOME_DIR', '../apache-zookeeper-3.6.2-bin'))

# LIBS_DIR - Optional. The directory in which the jar is located.
LIBS_DIR = Path(os.environ.get('LIBS_DIR', '../build/libs'))


class ZkCli:
    def __init__(self, *args):
        self.args = args
        self.log_dir = tempfile.TemporaryDirectory()

        self.p = None
        self.log_text = None
        self.output = None

    def exec(self):
        self.p = subprocess.Popen(
            ("bin/zkCli.sh",) + self.args,
            cwd=ZK_HOME_DIR,
            env={
                "ZOO_LOG_DIR": self.log_dir.name,
                "ZOO_LOG4J_PROP": "INFO,ROLLINGFILE"
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        return self

    def wait(self, timeout=None):
        assert type(self.p) is subprocess.Popen

        output = b""
        while True:
            line = self.p.stdout.readline()
            if not line:
                break
            output += line

        self.output = output

        result = self.p.wait(timeout=timeout)

        log_file = next(Path(self.log_dir.name).glob("zookeeper-*-cli-*.log"))
        self.log_text = log_file.read_text()

        self.log_dir.cleanup()
        return result


def zk_parse_ls(output: bytes) -> List[str]:
    res_line = output.split(b"\n")[-2]
    res_line_txt = res_line.decode('utf8')
    assert res_line_txt.startswith("[") and res_line_txt.endswith("]")
    res_line_txt = res_line_txt[1:-1]
    return res_line_txt.split(", ")


@pytest.fixture
def zookeeper():
    connection_string = ZK_CONNECTION_STRING
    if not connection_string:
        starter = subprocess.Popen(["bin/zkServer.sh", "start"], cwd=ZK_HOME_DIR)
        starter.wait()
        connection_string = "127.0.0.1:2181"

    yield connection_string

    # stopper = subprocess.Popen(["bin/zkServer.sh", "stop"], cwd=ZK_PATH)
    # stopper.wait()


@pytest.fixture
def jar_path():
    jar_output = Path("zgroups-1.0-SNAPSHOT.jar")
    return LIBS_DIR / jar_output


@pytest.fixture
def scale_config(request, zookeeper):
    conf_name = "/celery"

    conf = request.param
    assert isinstance(conf, dict)

    conf_string = ";".join([f"{worker}:{scale}" for worker, scale in conf.items()])
    starter = ZkCli("-server", zookeeper, "create", conf_name, conf_string)
    assert 0 == starter.exec().wait()

    yield conf, conf_name

    starter = ZkCli("-server", zookeeper, "delete", conf_name)
    assert 0 == starter.exec().wait()


@pytest.mark.parametrize("scale_config", (
        {"queue1": 2, "queue2": 1},
        {"queue1": 2, "queue2": 2},
        {"queue1": 10, "queue2": 5},
), indirect=True)
@pytest.mark.parametrize("scale", (
        2, 5, 50,
))
def test_load(zookeeper, scale: int, jar_path: str, scale_config: Tuple[dict, str]):
    conf, conf_name = scale_config

    ps = []
    for i in range(scale):
        p = subprocess.Popen(
            ["java", "-jar", jar_path, zookeeper, conf_name]
        )
        ps.append(p)

    time.sleep(scale * 0.5)

    starter = ZkCli("-server", zookeeper, "ls", conf_name)
    assert 0 == starter.exec().wait()
    ls = zk_parse_ls(starter.output)

    assert len(ls) == min(scale, sum(conf.values()))

    for i, p in enumerate(ps):
        print("killing", i)
        p.send_signal(signal.SIGINT)

    for i, p in enumerate(ps):
        p.wait(timeout=5)

    starter = ZkCli("-server", zookeeper, "ls", conf_name)
    assert 0 == starter.exec().wait()


@pytest.mark.parametrize("scale_config", (
        {"queue1": 2, "queue2": 3},
        {"queue1": 1, "queue2": 3, "queue3": 1},
), indirect=True)
@pytest.mark.parametrize("scale", (5,))
def test_command(zookeeper, scale: int, jar_path: str, scale_config: Tuple[dict, str], tmp_path):
    conf, conf_name = scale_config

    ps = []
    for i in range(scale):
        p = subprocess.Popen(
            ["java", "-jar", jar_path, zookeeper, conf_name, "sh", "-c", f"printf $ZGROUPS_GROUP > {tmp_path}/{i}"],
        )
        ps.append(p)

    time.sleep(scale * 0.5)

    for i, p in enumerate(ps):
        print("killing", i)
        p.send_signal(signal.SIGINT)

    results = []
    for i, p in enumerate(ps):
        p.wait(timeout=5)
        results.append(open(tmp_path/str(i), 'r').read())

    assert Counter(results) == conf
