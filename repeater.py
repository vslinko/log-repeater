import asyncio
import re
import sys
import ssl

ssl_context = ssl.SSLContext()
ssl_context.verify_mode = ssl.CERT_NONE

line_re = re.compile(r'^(?P<method>[^ ]+) (?P<path>[^ ]+) (?P<host>[^ ]+) (?P<code>[^ ]+) (?P<body>.+)$')
http_response_re = re.compile(r'^HTTP/1.1 (\d+) .+$')

async def make_request(req):
  async def _task():
    reader, writer = await asyncio.open_connection(
      req['connection']['host'],
      req['connection']['port'],
      ssl=ssl_context if req['connection']['ssl'] else None
    )

    msg = ""
    msg += "{} {} HTTP/1.1\r\n".format(req['request']['method'], req['request']['path'])
    for name, value in req['request']['headers']:
      msg += "{}: {}\r\n".format(name, value)
    msg += "\r\n"

    if req['request']['body']:
      msg += req['request']['body']

    writer.write(msg.encode())
    await writer.drain()

    result = await reader.readline()
    writer.close()
    return result

  try:
    result = await asyncio.wait_for(_task(), timeout=3)

    match = http_response_re.match(result.decode())
    if match:
      code = match.group(1)

      if code == req['assert_code']:
        print("OK: {} {}, expected {}".format(req['request']['method'], req['request']['path'], req['assert_code']))
      else:
        print("FAIL: {} {}, expected {}, got {}\nbody: {}".format(req['request']['method'], req['request']['path'], req['assert_code'], code, req['request']['body']))
  except asyncio.TimeoutError:
      print("FAIL: {} {}, timeout\nbody: {}".format(req['request']['method'], req['request']['path'], req['request']['body']))

async def _follow():
  loop = asyncio.get_running_loop()
  reader = asyncio.StreamReader()
  reader_protocol = asyncio.StreamReaderProtocol(reader)
  await loop.connect_read_pipe(lambda: reader_protocol, sys.stdin)

  while not reader.at_eof():
    line = await reader.readline()

    if line:
      yield line.decode()

  print('w')


async def follow():
  while True:
    line = sys.stdin.readline()

    if not line:
      await asyncio.sleep(0.1)
      continue

    yield line

def convert_req(log):
  body = None
  headers = [
    ('Host', 'api.cian.ru')
  ]

  if log['body'] != '-':
    body = log['body']
    headers.append(('Content-Type', 'application/json'))
    headers.append(('Content-Length', len(log['body'])))

  return {
    'connection': {
      'host': 'api.cian.ru',
      'port': 443,
      'ssl': True
    },
    'request': {
      'method': log['method'],
      'path': '/search-offers/' + log['path'].replace("/public/", ""),
      'headers': headers,
      'body': body
    },
    'assert_code': log['code']
  }

def filter_log(log):
  if log['code'] == 499:
    return False

  if not '/v1/search-offers-desktop/' in log['path']:
    return False

  return True

async def worker(name, queue):
  while True:
    req = await queue.get()
    print(name)
    await make_request(req)
    queue.task_done()

async def main():
  queue = asyncio.Queue()
  tasks = []

  for i in range(10):
    task = asyncio.create_task(worker(f'worker-{i}', queue))
    tasks.append(task)

  async for line in follow():
    match = line_re.match(line)

    if match:
      req = match.groupdict()

      if filter_log(req):
        queue.put_nowait(convert_req(req))

  print('join')
  await queue.join()

  for task in tasks:
    task.cancel()

  print('cancel')
  await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
  asyncio.run(main())
