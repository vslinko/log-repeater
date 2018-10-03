import asyncio
import re
import time

import sys

LOG_RE_MAP = {'method': 0, 'path': 1, 'host': 2, 'code': 3, 'body': 4}
LOG_RE = re.compile(r'^.*nginx: ([^ ]+) ([^ ]+) ([^ ]+) (\d+) (.*)$')

class DoubleNode():
  def __init__(self, val):
    self.val = val
    self.prev = None
    self.next = None

class DoubleList():
  def __init__(self):
    self._first = None
    self._last = None
    self._len = 0

  def append(self, val):
    node = DoubleNode(val)
    self.append_node(node)
    return node

  def append_node(self, node):
    assert(node.prev == None)
    assert(node.next == None)

    if self._len == 0:
      self._first = node
      self._last = node
    else:
      self._last.next = node
      node.prev = self._last
      self._last = node

    self._len += 1

    return node

  def remove_node(self, node):
    assert(len([n for n in self._iter_nodes() if n == node]) == 1)

    if node.prev:
      assert(node.prev.next == node)
      node.prev.next = node.next
      assert(node.prev.next == node.next)

    if node.next:
      assert(node.next.prev == node)
      node.next.prev = node.prev
      assert(node.next.prev == node.prev)

    node.prev = None
    node.next = None

    self._len -= 1

    return node

  def pop(self):
    if self._len == 0:
      return

    node = self._first

    if self._len == 1:
      self._first = None
      self._last = None
    else:
      self._first = node.next
      self._first.prev = None
      node.next = None

    self._len -= 1

    return node

  def __iter__(self):
    for node in self._iter_nodes():
      yield node.val

  def _iter_nodes(self):
    l = 0
    m = ""
    node = self._last
    while node:
      m += "{} {} {}\n".format(node.prev, node, node.next)
      yield node
      l += 1
      if l > self._len:
        print(m)
        raise Exception('list bug')
      node = node.prev

  def __len__(self):
    return self._len

class LRU():
  def __init__(self, size=100):
    self._list = DoubleList()
    self._map = {}
    self._size = size

  def store(self, log):
    log_hash = LRU._hash(log)

    if log_hash in self._map:
      node = self._map[log_hash]
      self._list.remove_node(node)
      self._list.append_node(node)
    else:
      node = self._list.append(log)
      self._map[log_hash] = node

    if len(self._list) > self._size:
      self._gc()

  def __iter__(self):
    for val in self._list:
      yield val

  def _gc(self):
    node = self._list.pop()
    del self._map[LRU._hash(node.val)]

  def _status(self):
    log_size = 0
    for node in self._list._iter_nodes():
      log_size += sys.getsizeof(node)
      log_size += sys.getsizeof(node.val)
      log_size += sys.getsizeof(node.val[0])
      log_size += sys.getsizeof(node.val[1])
      log_size += sys.getsizeof(node.val[2])
      log_size += sys.getsizeof(node.val[3])
      log_size += sys.getsizeof(node.val[4])

    map_size = sys.getsizeof(self._map)
    for key in self._map.keys():
      map_size += sys.getsizeof(key)

    return {
      'log_len': len(self._list),
      'map_size': map_size,
      'log_size': log_size
    }

  def _hash(log):
    return '{} {} {} {}'.format(log[LOG_RE_MAP['method']], log[LOG_RE_MAP['path']], log[LOG_RE_MAP['host']], log[LOG_RE_MAP['body']])

lru = LRU()

def cc(text):
  def decode_body(match):
    code = match.group(1)
    try:
      return bytearray.fromhex(code).decode()
    except UnicodeDecodeError:
      print('')
      print('UnicodeDecodeError')
      print(text)
      print('')

  return decode_body


class SyslogServerProtocol:
  def connection_made(self, transport):
    pass

  def datagram_received(self, data, addr):
    a = time.process_time_ns()
    match = LOG_RE.match(data.decode('unicode_escape'))

    if not match:
      return

    log = match.groups()
    lru.store(log)

    if len(writers_to_spread) > 0:
      asyncio.create_task(self._spread(log))

    b = time.process_time_ns()
    print('received {} ns'.format(b - a))

  async def _spread(self, log):
    for writer in writers_to_spread:
      try:
        writer.write('{} {} {} {} {}\n'.format(log[0], log[1], log[2], log[3], log[4]).encode())
        await writer.drain()
      except (BrokenPipeError, ConnectionResetError):
        writers_to_spread.remove(writer)

async def cache_handler(reader, writer):
  for log in lru:
    writer.write('{} {} {} {} {}\n'.format(log[0], log[1], log[2], log[3], log[4]).encode())

  await writer.drain()
  writer.close()

writers_to_spread = []

async def log_handler(reader, writer):
  writers_to_spread.append(writer)

async def server_handler(reader, writer):
  command = (await reader.readline()).decode().rstrip()

  if command == 'cache':
    await cache_handler(reader, writer)
  elif command == 'log':
    await log_handler(reader, writer)
  else:
    writer.close()

async def main():
  loop = asyncio.get_running_loop()

  transport, protocol = await loop.create_datagram_endpoint(
    lambda: SyslogServerProtocol(),
    local_addr=('0.0.0.0', 9999)
  )

  server = await asyncio.start_server(
    server_handler,
    '0.0.0.0',
    9999
  )

  await server.serve_forever()

if __name__ == "__main__":
  asyncio.run(main())
