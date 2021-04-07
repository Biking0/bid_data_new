import time

def clock(func):
    def clocked(*args, **kwargs):
        t1 = time.time()
        print(121232133)
        result = func(*args, **kwargs)

        print('dfge5g')
        t2 = time.time()
        elapsed = t2 - t1
        print("This func cost: {}seconds".format(elapsed))
        return result
    return clocked

@clock
def test():
    print(123)

test()