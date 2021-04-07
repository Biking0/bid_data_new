class CountDown:

    def __init__(self, step):
        self.step = step

    def __next__(self):
        if self.step <= 0:
            raise StopIteration
        print(self.step)
        self.step -= 1
        return self.step

    def __iter__(self):
        print(self)
        return self


test = CountDown(3)

test.__next__()
test.__iter__()
