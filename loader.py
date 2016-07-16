import psycopg2
import sys
import datetime

CHUNK_SIZE = 10000
TABLE_NAME = 'crimes'

conn = psycopg2.connect('dbname=loader_test user=blist')
cur = conn.cursor()

print sys.argv
if sys.argv.length != 2:
  print 'path to ldjson file plz'

row_idx = 0
for line in open(sys.argv[1]):
  chunk_idx = row_idx / CHUNK_SIZE
  chunk_offset = row_idx % CHUNK_SIZE
  cur.execute('INSERT INTO ' + TABLE_NAME + ' (row_idx, chunk_idx, data) VALUES (%s, %s, %s)', (row_idx, chunk_idx, line))
  if chunk_offset == 0:
    cur.execute('UPDATE uploads SET latest_chunk_idx = %s WHERE table_name = %s', (chunk_idx, TABLE_NAME))
    print 'committing...'
    conn.commit()
    print 'up to', row_idx
  row_idx += 1

cur.execute('UPDATE uploads SET finished_upload_at = %s WHERE table_name = %s', (datetime.datetime.now(), TABLE_NAME))
conn.commit()
