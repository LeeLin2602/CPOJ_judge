import json
import time

class API():
    
    def resolve(self, data):
        raise NotImplementedError()
    
    def control(self):
        raise NotImplementedError()
    
    def respond(self):
        raise NotImplementedError()

    def __init__(self, taskq, resq, waitq):
        self.taskq = taskq
        self.resq = resq
        self.waitq = waitq

class API_return(API):

    def resolve(self, data):
        self.status     = int(data['status'])
        self.submission = int(data['submission'])
        self.submitter  = int(data['submitter'])
        self.problem    = int(data['problem'])
        self.result     = data['result']
        
    def control(self):
        if self.result:
            with open("/var/www/judger/results/%d.json" % (self.submission), "w") as f:
                f.write(json.dumps(self.result))
        self.resq.put((self.status == 2, self.submission, self.submitter, self.problem, self.status))

    def respond(self):
        response = {
                'status': 'ok'
            }

        return response

class API_fetch(API):

    def resolve(self, data):
        self.num = int(data["num"])
    
    def control(self):
        self.count = 0
        self.assign_jobs = []
        # print(self.taskq.qsize())
        for i in range(self.num):
            if self.taskq.qsize() > 0:
                cur = self.taskq.get()
                self.assign_jobs.append(cur)
                self.waitq.append((time.time(),cur))
                self.count += 1
            else:
                return

    def respond(self):
        response = {
                "status": "ok", 
                "data": {
                    "num": self.count, 
                    "tasks": self.assign_jobs
                    }
                }
        return response

