class Conn():
    def __init__(self):
        self.data = 'data'

    def close(self):
        print('close')


class Pool():
    def __init__(self, max, min, *args, **kwargs):
        self.max = max
        self.min = min

        # 现役兵
        self._used = {}
        # 预备兵
        self._pool = []
        # 士兵ID
        self._key = 0
        # 标示战争结束没
        self.closed = False

        for i in range(min):
            self._create_conn()

    def _get_key(self):
        """"给现役兵一个ID"""
        self._key += 1
        return self._key

    def _create_conn(self, key=None):
        """培养兵"""
        con = Conn()
        if key:
            self._used[key] = con
        else:
            self._pool.append(con)
        return con

    def get_conn(self, key=None):
        """使用兵
            ID不空：取现役兵
            ID为空：给预备兵创建ID，再使用
        """
        if self.closed:
            raise UserWarning

        if key in self._used:
            return self._used[key]

        if not key:
            key = self._get_key()

        if self._pool:
            self._used[key] = self._pool.pop()
            return self._used[key]
        else:
            if len(self._used) == self.max:
                raise UserWarning
            # 培养一个兵吧
            return self._create_conn(key)

    def put_conn(self, conn, key):
        """回退到预备役"""
        if self.closed:
            raise UserWarning

        if len(self._pool) < self.min:
            self._pool.append(conn)
        else:
            conn.close()

        del self._used[key]

    def close_all(self):
        """战争结束"""
        if self.closed:
            raise UserWarning

        for conn in self._pool + list(self._used.values()):
            try:
                conn.close()
            except Exception:
                pass
        self.closed = True


class ThreadPool(Pool):
    def __init__(self, max, min, *args, **kwargs):
        import threading
        Pool.__init__(self, max, min, *args, **kwargs)
        self._lock = threading.Lock()

    def get(self, key=None):
        self._lock.acquire()
        try:
            self.get_conn(key)
        finally:
            self._lock.release()

    def put(self, conn, key):
        self._lock.acquire()
        try:
            self.put_conn(conn, key)
        finally:
            self._lock.release()

    def close(self):
        self._lock.acquire()
        try:
            self.close_all()
        finally:
            self._lock.release()
