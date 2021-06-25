#!/usr/local/bin/python3.9

# from multiprocessing import Process, Queue
import pymysql, socket
import time, os, json
import _thread
from queue import Queue
from collections import deque
from heapq import *

import api

verbose = 3

error_handler_print = lambda e: print(type(e).__name__+":", e) and 0
error_handler_noretry = lambda e: print(type(e).__name__+":", e) or 1
error_handler_wait = lambda e: time.sleep(2) or error_handler_print(e)
printv = lambda msg: verbose and print(msg)
printvv = lambda msg: (verbose >= 2) and print(msg)
printvvv = lambda msg: (verbose >= 3) and print(msg)

def trying(func, args = tuple(), kwargs = dict(), handler = lambda e: 0):
    while True:
        try:
            val = func(*args, **kwargs)
            return val
            break
        except KeyboardInterrupt:
            print("Keyboard Interrupted.")
            exit(0)
        except Exception as e:
            if handler(e):
                break

class SQL():

    SQL_HOST = "10.5.5.1"
    SQL_USERNAME = "oj"
    SQL_PASSWORD = "password"
    SQL_DB = "oj"   

    def __connect(self):
        config = {
                "host": self.SQL_HOST,
                "user": self.SQL_USERNAME, 
            "password": self.SQL_PASSWORD, 
            "database": self.SQL_DB}

        return trying(pymysql.connect, 
                      kwargs = config, 
                      handler = error_handler_wait)
    
    def put_back(self):
        while True:
            # if len(self.waitq): print(time.time(), self.waitq)
            while len(self.waitq) and time.time() >= 180 + self.waitq[0][0]:
                if self.waitq[0][1][0] in self.donetasks: continue
                self.taskq.put(self.waitq[0][1])
                self.resq.put((1, self.waitq[0][1][0], 0, 0, 12))
                printvv("Server: Submission" + str(self.waitq[0][1][0]) + " timeout.")
                self.waitq.popleft()
            time.sleep(1)

    def fetch_and_update_once(self):
        cur = self.con.cursor()
        num = cur.execute("SELECT ID, Language, ProblemID, Submitter FROM `Solutions` WHERE `Status` = 0")
        
        if num != 0:
            printvv("Server: Take %d submissions into the queue" % (num))

        for i in cur.fetchall():
            cur.execute("UPDATE `Solutions` SET `Status`=12 WHERE `ID`=%d" % (i[0]))
            self.taskq.put(i)
        
        while self.resq.qsize():
            mode, sid, uid, pid, status = self.resq.get()
            if status >= 2 and status < 12: self.donetasks.add(sid)
            try:
                cur.execute("UPDATE `Solutions` SET `Status`=%d WHERE `ID`=%d;" % (status, sid))
                
                if mode == 0: # verdict AC
                    cur.execute("UPDATE `Problems` SET `AC_Times`=AC_Times+1 WHERE `ID`=%d" % pid) 
                    cur.execute("UPDATE `Accounts` SET `AC_Times`=AC_Times+1 WHERE `ID`=%d" % uid)
            except Exception as e:
                self.resq.put((mode, sid, uid, pid, status))
                printv("Server: Error on SQL,", e)
        
        self.con.commit()
 
    def fetch_and_update(self):
        while True:
            self.fetch_and_update_once()
            time.sleep(2)

    def __init__(self, taskQue, resQue, waitQue):
        self.con = self.__connect()
        self.taskq = taskQue
        self.resq = resQue
        self.waitq = waitQue
        self.donetasks = set()
        printv("SQL: connection established.")
    
    def close(self):
        cur = self.con.cursor()
        cur.execute("UPDATE `Solutions` SET `Status` = 0 WHERE `Status` = 0 OR `Status` = 1 OR `Status` = 12");
        self.con.commit()
        self.con.close()
        printv("SQL: disconnected.")

class Authenticator():
    
    PRESHARED_KEY = "presharedkey"
    
    def __init__(self):
        pass

    def authenticate(self, key):
        return key == self.PRESHARED_KEY

class Server():
    
    BINDING_HOST = "10.5.5.1"
    BINDING_PORT = 3389
    APIs = {"fetch": api.API_fetch, "return": api.API_return}
    
    
    def __rcv_request(self, request):

        try:
            # print(request)
            request = json.loads(request) 
        except json.JSONDecodeError:
            raise ValueError("not a json.")
        
        try:
            serial  = request["serial"]
            wkey    = request["worker_key"]
            action  = request["action"]
            data    = request["data"]
        except Exception:
            raise ValueError("missing neccesary fields.")

        if not self.auth.authenticate(wkey):
            raise ValueError("Server: authentication failed. Unauthorized.")
        
        if action not in self.APIs:
            raise ValueError("unvalid `action` field.")

        try:
            ctl = self.APIs[action](self.taskq, self.resq, self.waitq)
            ctl.resolve(data)
            ctl.control()
            response = ctl.respond()
            response['serial'] = serial
        except NotImplementedError as e:
            raise ValueError("not implemented error.")
        except Exception as e:
            raise ValueError(str(e))

        try:
            response = json.dumps(response)
            return response
        except Exception as e:
            raise ValueError("wrong API implementation, " + str(e))

    def __connect(self, connection):
        # connection.send(str.encode("judge_controller: " + self.PRESHARED_KEY + "\r\n"))
        
        request = ""
        request_parser = []

        while True:
            try:
                if request: connection.settimeout(5)
                newData = connection.recv(4096).decode()
                connection.settimeout(None)
                for i in newData:
                    if (i in ["{", "[", "("]) and (len(request_parser) == 0 or request_parser[-1] != "\""):
                        request_parser.append(i)
                    elif i == "\"":
                        if len(request_parser) == 0 or request_parser[-1] != "\"":
                            request_parser.append(i)
                        else:
                            request_parser.pop()
                    elif i in ["}", "]", ")"] and (len(request_parser) == 0 or request_parser[-1] != "\""):
                        if len(request_parser) == 0 or request_parser[-1] != {"}": "{", "]": "[", ")": "("}[i]:
                            printvvv("Server: Invalid request form")
                            request_parser = []
                            request = ""
                            continue
                        else:
                            request_parser.pop()
                    request += i
                    if len(request_parser) == 0:
                        try:
                            connection.sendall(self.__rcv_request(request).encode())    
                        except ValueError as e:
                            printvvv("Server: Failed to give a response, " + str(e))
                        request = ""

            except socket.timeout:
                if request:
                    printvvv("Server: Socket timeout.")
                    request = ""
            except Exception as e:
                error_handler_print(e)
                connection.close()
                return
        
        connection.close()

    def launch(self):
        trying(self.socket.bind, args = ((self.BINDING_HOST, self.BINDING_PORT),), handler = error_handler_wait)
        trying(self.socket.listen, handler = error_handler_wait)
        printv("Server: Socket server launched.")
    
    def accept(self):
        while True:
            try:
                conn, addr = self.socket.accept()
                printv("Server: Connected to %s:%d" % addr)
                _thread.start_new_thread(self.__connect, (conn, ))
            except KeyboardInterrupt:
                printv("Keyboard Interrupted.")
                break
            except Exception as e:
                error_handler_print(e)

    def __init__(self, taskq, resq, waitq):
        self.taskq = taskq
        self.resq = resq
        self.waitq = waitq
        self.socket = socket.socket()
        self.auth = Authenticator()
       
    def close(self):
        self.socket.close()
        printv("Server: Socker server stopped.")

def main():
    # init thread-shared resource
    taskq = Queue()
    resq = Queue()
    waitq = deque([])

    # init service components
    sql = SQL(taskq, resq, waitq)
    server = Server(taskq, resq, waitq)
    
    # launch the service
    server.launch()
    _thread.start_new_thread(sql.put_back, tuple())
    _thread.start_new_thread(sql.fetch_and_update, tuple())
    _thread.start_new_thread(server.accept, tuple())
    while True:
        try:
            N = input()
            if N == "END":
                break
        except KeyboardInterrupt:
            print("Keyboard Interrupted.")
            break
    
    sql.close()
    server.close()
    
        
main()


