import time
from collections import deque

class CancelBackoff:

    def __init__(self, base_seconds=2, factor=2, window_seconds=120, max_seconds=None):
        self.base = float(base_seconds)  # 初始等待时间
        self.factor = float(factor)  # 指数退避的倍数
        self.window = float(window_seconds)  # 窗口时间
        self.max_seconds = None if max_seconds is None else float(max_seconds)  # 最大等待时间
        self._events = deque()  # 存储事件的队列

    def next_sleep(self):
        now = time.monotonic()

        # 清理窗口外的事件
        cutoff = now - self.window
        while self._events and self._events[0] <= cutoff:
            self._events.popleft()

        # 记录本次 cancel
        self._events.append(now)

        # 计算退避时间
        retries = len(self._events) - 1  # 重试次数
        sec = self.base * (self.factor ** retries)

        if self.max_seconds is not None:
            sec = min(sec, self.max_seconds)  # 如果有最大限制时间，取最小值

        return sec
