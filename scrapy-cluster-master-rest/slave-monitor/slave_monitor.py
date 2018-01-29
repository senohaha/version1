# -*- coding: utf-8 -*-
import select
import socket
import Queue
import redis
import time

class SlaveMonitor(object):

    def __init__(self, redis_conn):
        self.redis_conn = redis_conn
        # create a socket
        # 创建一个socket
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(False)  # 非阻塞
        # set option reused
        # 端口复用 sol_socket  so_reuseadddr
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.server_address = ('192.168.1.222', 9011)
        self.server.bind(self.server_address)

        self.server.listen(10)

        # sockets from which we except to read
        # 存 我们想去读 的 socket
        self.inputs = [self.server]

        # sockets from which we expect to write
        # 存 我们想去写的 socket
        self.outputs = []

        # Outgoing message queues (socket:Queue)
        # 要发出去的消息队列
        self.message_queues = {}

        # A optional parameter for select is TIMEOUT
        self.timeout = None

    def select_socket(self):

        while self.inputs:
            print "##################waiting for next event##############"
            readable, writable, exceptional = select.select(self.inputs, self.outputs, self.inputs, self.timeout)
            # 如果监听的socket（参数）满足可读可写的条件，则返回readable或writable就会有值

            print readable, ';   ', writable, ';   ', exceptional, ';'

            # When timeout reached , select return three empty lists
            if not (readable or writable or exceptional):
                print "Time out ! "
                break

            print '----------readable---------------'
            for s in readable:
                if s is self.server:
                    # A "readable" socket is ready to accept a connection
                    # s: server 的 socket对象 用于 等待接受外界连接的 socket
                    connection, client_address = s.accept()  # 一个客户端socket(s) 执行 s.connect ,
                    # 这边的server的 socket（s） s.accept后 会生成 服务器 唯一识别该客户端 的 socket对象(connection)
                    connection.setblocking(0)
                    self.inputs.append(connection)
                    # [<socket._socketobject object at 0x7f59b8809590>, <socket._socketobject object at 0x7f59b88097c0>]
                    self.message_queues[connection] = Queue.Queue()
                else:
                    data = s.recv(1024)
                    if data:
                        print "received data ", data, "from ", s.getpeername()  # <type 'tuple'>

                        ip = s.getpeername()[0]
                        key = 'stats:{ip}:info'.format(ip=ip)
                        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        self.redis_conn.delete(key)
                        self.redis_conn.rpush(key, 'online', data, current_time)

                        self.message_queues[s].put(data)
                        # # Add output channel for response
                        # if s not in outputs:
                        #     outputs.append(s)

                    else:
                        # Interpret empty result as closed connection
                        print "  closing", client_address

                        ip = s.getpeername()[0]
                        key = 'stats:{ip}:info'.format(ip=ip)
                        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                        self.redis_conn.delete(key)
                        self.redis_conn.rpush(key, 'offline', current_time)

                        if s in self.outputs:
                            self.outputs.remove(s)
                        self.inputs.remove(s)
                        s.close()
                        # remove message queue
                        del self.message_queues[s]

            print '----------writeable---------------'
            for s in writable:
                try:
                    next_msg = self.message_queues[s].get_nowait()  # 非阻塞的向QUEUE里取数据，若无数据，则不等待直接抛出异常
                except Queue.Empty:
                    print " ", s.getpeername(), 'queue empty'
                    self.outputs.remove(s)
                else:
                    print " sending ", next_msg, " to ", s.getpeername()
                    s.send(next_msg)
            print '----------exception------------'
            for s in exceptional:
                print " exception condition on ", s.getpeername()
                # stop listening for input on the connection
                self.inputs.remove(s)
                if s in self.outputs:
                    self.outputs.remove(s)
                s.close()
                # Remove message queue
                del self.message_queues[s]

    def run(self):
        self.select_socket()


if __name__ == '__main__':
    redis_conn = redis.Redis()
    d = SlaveMonitor(redis_conn=redis_conn)
    d.run()