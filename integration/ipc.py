import logging
import os
import signal
from itertools import count

import zmq

ctx = zmq.Context()


def create_killer(ipc_path):
    killer = ctx.socket(zmq.REP)
    killer.bind(f"tcp://127.0.0.1:5665")
    # killer.bind(f"ipc://{ipc_path}-kill.ipc")
    return killer


def node_main(ipc_path, name_):
    name = name_ # f"{os.environ['ZGROUPS_GROUP']}"

    logging.basicConfig(filename=ipc_path + f".{name}.log", level=logging.DEBUG,
                        format="{asctime} {message}", style="{")

    logging.info(f"{name}, {os.getpid()}, {ipc_path}")

    # while True:
    req = ctx.socket(zmq.REQ)
    # req.connect(f"ipc://{ipc_path}/node.ipc")
    req.connect(f"tcp://127.0.0.1:5666")

    # req.monitor("inproc://monitor-req")
    # mon = ctx.socket(zmq.PAIR)
    # mon.connect("inproc://monitor-req")

    logging.info("sending up")
    req.send_string(f"up {name}")
    logging.info("up sent")

    for i in range(10):
        if (req.poll(1000) & zmq.POLLIN) != 0:
            ok = req.recv_string()
            break
    else:
        raise TimeoutError()

    # req.setsockopt(zmq.LINGER, 0)
    # req.close()

    logging.info(ok)
    if ok != "OK":
        raise Exception(f"Go {ok}")

    def report_dying(*args):
        logging.info("sending down")
        req.send_string(f"down {name}")
        logging.info("down sent")
        ok = req.recv_string()
        logging.info(ok)

        req.close(0)

    # Create a request when dying
    signal.signal(signal.SIGINT, report_dying)
    signal.signal(signal.SIGTERM, report_dying)

    killer = ctx.socket(zmq.REQ)
    # killer.connect(f"ipc://{ipc_path}-kill.ipc")
    killer.connect(f"tcp://127.0.0.1:5665")

    killer.send_string(name)
    killer.recv_string()
    report_dying()


def srv_main(ipc_path, scale):
    logging.basicConfig(filename=ipc_path + ".log", level=logging.DEBUG,
                        format="{asctime} {message}", style="{")
    live = set()
    dead = set()

    def handle(stat, node):
        if stat == 'up':
            if node in live | dead:
                return False
            live.add(node)
        if stat == "down":
            if node not in live or node in dead:
                return False
            live.remove(node)
            dead.add(node)
        return True

    sock = ctx.socket(zmq.REP)
    # sock.bind(f"ipc://{ipc_path}/node.ipc")
    sock.bind(f"tcp://127.0.0.1:5666")

    try:
        for i in count():
            logging.info(f"RECV")
            msg = sock.recv_string()
            logging.info(f"RECVED {msg}")

            stat, node = msg.split(" ")
            res = handle(stat, node)
            logging.info(f"RESP {res}")
            if res:
                sock.send_string("OK")
            else:
                sock.send_string("ERR")

            logging.info(f"SENT {res}")

            print(i, "live", live)
            print(i, "dead", dead)
            logging.info(f"rolling1")
            logging.info(f"rolling2")

            if len(dead) == scale:
                return
    finally:
        sock.close()
        logging.info(f"finally")

def main():
    from sys import argv

    if argv[1] == "node":
        node_main(argv[2], argv[3])
    if argv[1] == "srv":
        srv_main(argv[2], int(argv[3]))


if "__main__" == __name__:
    main()
