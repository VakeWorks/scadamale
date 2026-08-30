"""Lab 1 -- measure alpha (latency) and beta (per-byte cost) between two
containers, then FIT the book's cost model  T(n) = alpha + beta * n  by
least squares.

Two roles, one script:
    python3 measure.py --serve                # in the 'server' container
    python3 measure.py --client <host>        # in the 'client' container

The client sends messages of increasing size n, timing each round trip.
alpha is what a 1-byte message costs; beta is the slope as n grows. The
least-squares fit over all (n, T) pairs recovers both at once -- and R^2
tells you how well the straight-line model explains a real network.
"""

import socket
import statistics
import sys
import time

PORT = 9099
SIZES = [1, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216]
REPS = {1: 200, 1024: 100, 4096: 100, 16384: 50, 65536: 50,
        262144: 20, 1048576: 10, 4194304: 5, 16777216: 3}


def recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(min(1 << 20, n - len(buf)))
        if not chunk:
            raise ConnectionError("peer closed")
        buf.extend(chunk)
    return buf


def serve():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", PORT))
    s.listen(1)
    print("server: listening on :%d" % PORT, flush=True)
    while True:
        conn, addr = s.accept()
        try:
            while True:
                header = recv_exact(conn, 8)
                n = int.from_bytes(header, "big")
                if n == 0:
                    break
                recv_exact(conn, n)
                conn.sendall(b"\x01")          # 1-byte ack: echo cost stays ~alpha
        except ConnectionError:
            pass
        finally:
            conn.close()


def client(host):
    for attempt in range(30):                  # wait for the server to come up
        try:
            conn = socket.create_connection((host, PORT), timeout=5)
            break
        except OSError:
            time.sleep(0.5)
    else:
        sys.exit("client: server never came up")
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    points = []
    for n in SIZES:
        payload = bytes(n)
        times = []
        for _ in range(REPS[n]):
            t0 = time.perf_counter()
            conn.sendall(n.to_bytes(8, "big"))
            conn.sendall(payload)
            recv_exact(conn, 1)
            times.append(time.perf_counter() - t0)
        t = statistics.median(times)
        points.append((n, t))
        print("MEASURED n=%d median_rtt_us=%.1f (reps=%d)"
              % (n, t * 1e6, REPS[n]), flush=True)
    conn.sendall((0).to_bytes(8, "big"))
    conn.close()

    # least-squares fit T = alpha + beta*n (fm2: the 2x2 normal equations)
    m = len(points)
    sx = sum(n for n, _ in points)
    sy = sum(t for _, t in points)
    sxx = sum(n * n for n, _ in points)
    sxy = sum(n * t for n, t in points)
    beta = (m * sxy - sx * sy) / (m * sxx - sx * sx)
    alpha = (sy - beta * sx) / m
    ss_res = sum((t - (alpha + beta * n)) ** 2 for n, t in points)
    mean_y = sy / m
    ss_tot = sum((t - mean_y) ** 2 for _, t in points)
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    print("MEASURED alpha_us=%.2f" % (alpha * 1e6))
    print("MEASURED beta_ns_per_byte=%.4f (=> %.2f Gbit/s effective)"
          % (beta * 1e9, 8e-9 / beta if beta > 0 else float("inf")))
    print("MEASURED r_squared=%.4f" % r2)
    print("LOOK-AT-THIS: alpha is ~fixed cost per MESSAGE, beta per BYTE --"
          " so p-1 small messages cost ~(p-1)*alpha while one n-byte stream"
          " costs alpha+beta*n; fe4's tree-vs-flat aggregation trade-off is"
          " exactly these two numbers.")


if __name__ == "__main__":
    if "--serve" in sys.argv:
        serve()
    elif "--client" in sys.argv:
        client(sys.argv[sys.argv.index("--client") + 1])
    else:
        sys.exit("usage: measure.py --serve | --client <host>")
