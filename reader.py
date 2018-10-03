from optparse import OptionParser
import asyncio
import sys

async def main(command):
  try:
    reader, writer = await asyncio.open_connection(
      '127.0.0.1',
      9999
    )

    writer.write((command + '\n').encode())
    await writer.drain()

    while not reader.at_eof():
      line = await reader.readline()
      sys.stdout.write(line.decode())

  except Exception as e:
    sys.stderr.write(str(e) + '\n')
    sys.exit(1)

if __name__ == "__main__":
  parser = OptionParser("usage: %prog command")

  (options, args) = parser.parse_args()

  if len(args) != 1:
    parser.error("incorrect number of arguments")

  asyncio.run(main(args[0]))
