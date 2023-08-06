import time


class Timer:
    def __enter__(self):
        self.begin = time.perf_counter()

    def __exit__(self, type, value, traceback):
        self.end = time.perf_counter()
        self.interval = self.end - self.begin

    def __str__(self):
        return f"{self.interval * 1000:.0f} ms"
