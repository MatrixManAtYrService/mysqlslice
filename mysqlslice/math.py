class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __repr__(self):
        return "{}..{}".format(self.start, self.end)

    def __str__(self):
        return self.__repr__()

def make_intervals(start, end, size):
    boundaries = list(range(start, end, size))
    return [ Interval(x, x + size - 1) for x in boundaries ]


