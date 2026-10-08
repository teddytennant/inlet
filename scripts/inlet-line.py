#!/usr/bin/env python3
"""Send one JSON line to an inlet socket and print the reply line."""

import socket
import sys


def main() -> None:
    if len(sys.argv) != 3:
        sys.stderr.write("usage: inlet-line.py SOCK JSON\n")
        sys.exit(2)
    path, raw = sys.argv[1], sys.argv[2]
    if not raw.endswith("\n"):
        raw += "\n"
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    sock.sendall(raw.encode())
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    line = buf.split(b"\n", 1)[0]
    sys.stdout.buffer.write(line + b"\n")


if __name__ == "__main__":
    main()
