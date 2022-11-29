class Timer:
    def __init__(self):
        self.time = 0

    def reset_timer(self):
        self.time = 0

    def increment_timer(self):
        self.time += 1

timer = Timer()
