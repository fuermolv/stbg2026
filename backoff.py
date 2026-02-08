# import time
# from collections import deque

# class CancelBackoff:

#     def __init__(self, base_seconds=2, factor=2, window_seconds=120, max_seconds=None):
#         self.base = float(base_seconds)  # 初始等待时间
#         self.factor = float(factor)  # 指数退避的倍数
#         self.window = float(window_seconds)  # 窗口时间
#         self.max_seconds = None if max_seconds is None else float(max_seconds)  # 最大等待时间
#         self._events = deque()  # 存储事件的队列

#     def next_sleep(self):
#         now = time.monotonic()

#         # 清理窗口外的事件
#         cutoff = now - self.window
#         while self._events and self._events[0] <= cutoff:
#             self._events.popleft()

#         # 记录本次 cancel
#         self._events.append(now)

#         # 计算退避时间
#         retries = len(self._events) - 1  # 重试次数
#         sec = self.base * (self.factor ** retries)

#         if self.max_seconds is not None:
#             sec = min(sec, self.max_seconds)  # 如果有最大限制时间，取最小值

#         return sec





import time
from collections import deque

class CancelBackoff:
    def __init__(self, base_seconds=2, factor=2, window_seconds=90, max_seconds=None):
        self.base = float(base_seconds)
        self.factor = float(factor)
        self.window = float(window_seconds)
        self.max_seconds = None if max_seconds is None else float(max_seconds)
        self._events = deque()

        # 当前惩罚状态（带惯性）
        self._sec = self.base
        self._last_ts = None

    def penalty(self, n=1):
        now = time.monotonic()
        for _ in range(n):
            self._events.append(now)
            
    def next_sleep(self):
        now = time.monotonic()

        # 1) 时间衰减：k=2 → 每 (window/2) 秒衰减 1 个 factor
        if self._last_ts is not None:
            dt = now - self._last_ts
            if dt > 0:
                decay_power = dt * 2 / self.window
                self._sec = max(self.base, self._sec / (self.factor ** decay_power))
        self._last_ts = now

        # 2) 维护窗口内 cancel 事件
        cutoff = now - self.window
        while self._events and self._events[0] <= cutoff:
            self._events.popleft()
        self._events.append(now)

        # 3) 窗口惩罚（和你原始逻辑一致）
        retries = len(self._events) - 1
        target = self.base * (self.factor ** retries)

        # 4) 惩罚只能被抬高，不能瞬间变轻
        self._sec = max(self._sec, target)

        if self.max_seconds is not None:
            self._sec = min(self._sec, self.max_seconds)

        return round(self._sec, 2)
