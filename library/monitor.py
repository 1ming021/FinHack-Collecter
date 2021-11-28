import time
from library.alert import alert
import traceback
class tsMonitor:
    def __init__(self, func):
        self.func = func
 
    def __get__(self, instance, owner):
        def wrapper(*args, **kwargs):
            res=False
            while True:
                try:
                    res=self.func(*args,**kwargs)
                    break
                except Exception as e:
                    if "最多访问" in str(e):
                        print(self.func.__name__+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        info = traceback.format_exc()
                        print(self.func.__name__+":同步异常，"+str(info))
                        alert.send(self.func.__name__,'同步异常',str(info))
                        break
            print(self.func.__name__+":同步完毕")
            return res
        return wrapper