#!/usr/bin/env python
import socket


HOST = '192.168.1.222'   # server de ip
PORT = 9527

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect((HOST, PORT))
while True:
    message = raw_input('send message:>>')
    s.sendall(message)
    data = s.recv(1024)
    print data
s.close()