class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end
    def __str__(self):
        return self.start + ".." + self.end

def make_intervals(start, end, size):
    boundaries = list(range(start, end, size))
    intervals = [ Interval(x, x + size - 1) for x in boundaries ]


