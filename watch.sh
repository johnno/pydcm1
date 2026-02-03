#!/bin/sh
python3 -c "import socket, sys, select; s=socket.socket(); s.connect(('192.168.1.139',4999)); s.setblocking(False); [print(data.decode().replace('/>', '/>\n'), end='', flush=True) for _ in iter(lambda: select.select([s], [], [], 0.1), None) if select.select([s], [], [], 0)[0] for data in [s.recv(1024)] if data]"
