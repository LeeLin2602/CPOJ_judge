class API():
    
    def resolve(self, data):
        raise NotImplementedError()
    
    def control(self, req):
        raise NotImplementedError()
    
    def respond(self):
        raise NotImplementedError()

    def __init__(self, taskq):
        self.taskq = taskq

class API_fetch(API):

    def resolve(self, data):
        self.num = int(data["num"])
    
    def control(self, req):
        num = min(self.num, req['data']['num'])
        for i in range(num):
            self.taskq.put(req['data']['tasks'][i])

    def respond(self):
        response = {
                "action": "fetch",
                "data": {
                        "num": self.num 
                    }
                }
        return response

class API_return(API):
    
    def __init__(self):
        return None

    def resolve(self, data):
        self.status = data[0]
        self.submission_id = data[1][0]
        self.problem    = data[1][2]
        self.submitter  = data[1][3]
        self.result = data[3] if len(data) == 4 else {}

    def control(self, req):
        return 1
    
    def respond(self):
        response = {
                "action": "return",
                "data": {
                    "status": self.status,
                    "submission": self.submission_id,
                    "submitter": self.submitter,
                    "problem": self.problem,
                    "result": self.result
                    }
                }
        # print(response)
        return response
