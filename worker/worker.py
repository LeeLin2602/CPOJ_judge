#!/usr/bin/python3.9

import socket
import time, os, json
import _thread
from multiprocessing import Pool, Manager
from queue import Queue

import api, judge

verbose = 1
printv = lambda msg: verbose and print(msg)
printvv = lambda msg: (verbose >= 2) and print(msg)
printvvv = lambda msg: (verbose >= 3) and print(msg)

class Client():

    SERVER_IP   = "10.5.5.1"
    SERVER_PORT = 3389
    PRESHARED_KEY = "presharedkey"
    
    def __parse_request(self, req):
        try:
            req = json.loads(req)
        except json.JSONDecodeError:
            printvvv("Client: Not a json")
            raise ValueError

        # print(req)
        
        serial = req['serial']
        status = req['status']
        
        if status != 'ok':
            return
        
        self.history[serial].control(req)

        self.history[serial] = None
        

    def __rcv(self):
        req   = ""
        req_p = []
        while True:
            try:
                if req: self.socket.settimeout(5)
                newData = self.socket.recv(4096).decode()
                self.socket.settimeout(None)
                for i in newData:
                    if (i in ["{", "[", "("]) and (len(req_p) == 0 or req_p[-1] != "\""):
                        req_p.append(i)
                    elif i == "\"":
                        if len(req_p) == 0 or req_p[-1] != "\"":
                            req_p.append(i)
                        else:
                            req_p.pop()
                    elif i in ["}", "]", ")"] and (len(req_p) == 0 or req_p[-1] != "\""):
                        if len(req_p) == 0 or req_p[-1] != {"}": "{", "]": "[", ")": "("}[i]:
                            printvvv("Client: Invalid form")
                            req_p = []
                            req = ""
                            continue
                        else:
                            req_p.pop()
                    req += i
                    if len(req_p) == 0:
                        try:
                            self.__parse_request(req)
                        except ValueError as e:
                            printvvv("Client: Failed to parse request")
                        req = ""
            except socket.timeout:
                req = ""
                req_p = []
            except Exception as e:
                self.socket.close()
                print("Fatal Error: ", e)
                exit(1)
            
    def __init__(self):
        self.resque = Queue()
        self.reqque = Queue()
        self.timeoutq = []
        self.history = {}
        self.serial = 1

    def launch(self):
        while True:
            try:
                self.socket = socket.socket()
                self.socket.connect((self.SERVER_IP, self.SERVER_PORT))
                break
            except socket.error as e:
                print(type(e).__name__ + ": " + str(e))
                time.sleep(2)
        print("Client: Connected with controller.")
        _thread.start_new_thread(self.__rcv, tuple())
            
    def send_request(self, req):
        self.history[self.serial] = req
        req = req.respond()

        req['serial'] = self.serial
        req['worker_key'] = self.PRESHARED_KEY
        # heappush(self.timeoutq, (time.time() + 10, self.serial))
        # print(req)

        self.serial += 1
        req = json.dumps(req)
        self.socket.sendall(req.encode())


class Controller():
    
    MAX_PROCESS = 24
    
    def __fetch(self):
        while True:
            try:
                idle_num = self.MAX_PROCESS - self.taskq.qsize()
                if idle_num > 0: 
                    fetching = api.API_fetch(self.taskq)
                    fetching.resolve({"num": idle_num})
                    self.client.send_request(fetching)
            except Exception as e:
                print("Error: ", e)

            time.sleep(1)

    def __return(self):
        while True:
            try:
                while self.resq.qsize():
                    returning = api.API_return()
                    returning.resolve(self.resq.get())
                    self.client.send_request(returning)
            except Exception as e:
                print("Error: ", e)
            time.sleep(1)

    def __init__(self):
        self.client = Client()
        self.client.launch()
        
        self.path = judge.init(self.MAX_PROCESS)

        self.p = Pool(self.MAX_PROCESS)
        self.mng = Manager()
        self.taskq = self.mng.Queue()
        self.resq = self.mng.Queue()
        self.pstatus = self.mng.list([0] * self.MAX_PROCESS)
        _thread.start_new_thread(self.__fetch, tuple())
        _thread.start_new_thread(self.__return, tuple())

    def main_loop(self):
        while True:
            for i in range(self.MAX_PROCESS):
                if self.taskq.qsize() and not self.pstatus[i]:
                    self.pstatus[i] = 1
                    self.p.apply_async(judge.worker, args=(self.taskq.get(), i, self.path[i], self.resq, self.pstatus))
            time.sleep(2)

controller = Controller()
controller.main_loop()
