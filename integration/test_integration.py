import os
import random
import string
import subprocess
import signal
import tempfile
from datetime import datetime

from ipc import create_killer
import sys
from collections import Counter
from pathlib import Path
from typing import List, Tuple

import pytest

# ZK_CONNECTION_STRING - Optional. If not specified, the test will start a zookeeper server
import zmq

from log4j_template import log4j_file, log4j2_file

ZK_CONNECTION_STRING = os.environ.get('ZK_CONNECTION_STRING', "127.0.0.1:2181")

# ZK_HOME_DIR - Optional. The directory of the binary installation of zookeeper.
ZK_HOME_DIR = Path(os.environ.get('ZK_HOME_DIR', '../apache-zookeeper-3.7.0-bin'))

# LIBS_DIR - Optional. The directory in which the jar is located.
LIBS_DIR = Path(os.environ.get('LIBS_DIR', '../build/libs'))

JAR_NAME = 'zgroups-2.0-SNAPSHOT.jar'

JAR_PATH = str((LIBS_DIR / JAR_NAME).absolute())


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
    if res_line_txt == '':
        return []

    return res_line_txt.split(", ")


def terminate_all(ps: List[subprocess.Popen]) -> None:
    for i, p in enumerate(ps):
        p.send_signal(signal.SIGINT)


def _listen_process(conn_string, conf_name, sh_cmd, log_file_path=None):
    cmd = ["java", ]

    if log_file_path:
        log_conf = log4j_file % {"FILE_NAME": log_file_path}
        open(log_file_path + ".log4j.properties", "w").write(log_conf)
        cmd = cmd + [
            f'-Dlog4j.configuration=file:{log_file_path + ".log4j.properties"}',
        ]

        log_conf2 = log4j2_file % {"FILE_NAME": log_file_path}
        open(log_file_path + ".log4j2.properties", "w").write(log_conf2)
        cmd = cmd + [
            f'-Dlog4j.configurationFile={log_file_path + ".log4j2.properties"}'
        ]

    cmd = cmd + [
        "-jar", JAR_PATH,
        'listen', conn_string, conf_name, "/bin/sh", "-c", sh_cmd
    ]

    stds = subprocess.PIPE if log_file_path else None
    return subprocess.Popen(cmd, stdout=stds, stderr=stds)


@pytest.fixture
def listen_process():
    ps = []

    def factory(*args, **kwargs):
        p = _listen_process(*args, **kwargs)
        ps.append(p)
        return p

    yield factory

    terminate_all(ps)


@pytest.fixture
def zookeeper():
    connection_string = ZK_CONNECTION_STRING
    if not connection_string:
        starter = subprocess.Popen(["bin/zkServer.sh", "start"], cwd=ZK_HOME_DIR)
        starter.wait()
        connection_string = "127.0.0.1:2181"

    yield connection_string


@pytest.fixture
def scale_config(request, zookeeper):
    conf_name = "/" + ''.join([random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(7)])

    conf = request.param
    assert isinstance(conf, dict)

    conf_string = ";".join([f"{worker}:{scale}" for worker, scale in conf.items()])
    starter = ZkCli("-server", zookeeper, "create", conf_name, conf_string)
    assert 0 == starter.exec().wait(), starter.log_text

    yield conf, conf_name

    starter = ZkCli("-server", zookeeper, "delete", conf_name)
    assert 0 == starter.exec().wait()


@pytest.mark.parametrize("scale_config", (
        {"queue1": 2, "queue2": 1},
        {"queue1": 2, "queue2": 2, "queue3": 1},
        {"queue1": 10, "queue2": 5},
), indirect=True)
@pytest.mark.parametrize("scale", (
        2,
        5,
        30,
        # 40,
))
def test_load(tmp_path, scale, scale_config, zookeeper, listen_process):
    interpreter = sys.executable
    conf, conf_name = scale_config

    ipc_path = tmp_path / datetime.now().isoformat().replace(':', '-')
    ipc_path.mkdir()

    srv = subprocess.Popen(
        [interpreter, 'ipc.py', 'srv', str(ipc_path), str(scale)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    sock = create_killer(ipc_path)

    entries = {}
    for i in range(scale):
        proc = listen_process(zookeeper, conf_name, f"{interpreter} ipc.py node {ipc_path} {i}")
        # proc = subprocess.Popen([interpreter, "ipc.py", "node", str(ipc_path), str(i)], env={"ZGROUPS_GROUP": str(i)})
        entries[str(i)] = proc

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    idle = 30
    while entries and idle:
        socks = poller.poll(timeout=1000)

        if not socks:
            idle -= 1
            print("searching...", [p.args for p in entries.values()])
            continue
        else:
            print("recovered")
            idle = 30

        proc_id = sock.recv_string()
        sock.send_string("bye")
        print("got from ", proc_id)

        proc = entries.pop(proc_id)
        proc.send_signal(signal.SIGINT)

    if not idle:
        raise Exception("too long to find a working process")

    from select import select

    for i in range(scale * 2):
        r, _, _ = select([srv.stdout], [], [], 120)
        if r:
            live = srv.stdout.readline()[:-1]
            dead = srv.stdout.readline()[:-1]

            print(live.decode('utf8').replace(str(tmp_path), ""))
            print(dead.decode('utf8').replace(str(tmp_path), ""))
        else:
            break

    print(entries)
    srv.send_signal(signal.SIGINT)
    print("srv")
    out, err = srv.communicate()
    print("out", out.decode('utf8'))
    print("out", err.decode('utf8'))

    sock.close(0)

    starter = ZkCli("-server", zookeeper, "ls", conf_name)
    assert 0 == starter.exec().wait()


@pytest.mark.parametrize("scale_config", (
        {"queue1": 2, "queue2": 3},
        {"queue1": 1, "queue2": 3, "queue3": 1},
        {"queue1": 1, "queue2": 1, "queue3": 1},
        {"queue1": 1, "queue2": 1, "queue3": 0},
        {"queue1": 3, "queue2": 4, "queue3": 2, "queue4": 2, "queue5": 5},
), indirect=True)
def test_command(zookeeper, scale_config: Tuple[dict, str], tmp_path, listen_process):
    conf, conf_name = scale_config
    scale = sum(conf.values())

    ps = [
        listen_process(zookeeper, conf_name, f"printf $ZGROUPS_GROUP", str(Path(tmp_path / f"{i}.log")))
        for i in range(scale)
    ]

    results = []
    for i, p in enumerate(ps):
        # all lines are only 6 bytes long. careful if it's ever changed...
        line = p.stdout.read(6).decode('utf8')
        results.append(line)

    # Kill processes
    terminate_all(ps)

    assert dict(Counter(results)) == {k: v for k, v in conf.items() if v}
