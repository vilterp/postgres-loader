const pg = require('pg');
const express = require('express');
const bodyParser = require('body-parser');
const EventEmitter = require('events');
const Primse = require('bluebird');
const _ = require('lodash');

const app = express();
const server = require('http').Server(app);
const io = require('socket.io')(server);

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
  extended: true
})); 
app.use(require('morgan')('tiny'));

// serve static files
app.use(express.static(__dirname + '/public'));

// connect to postgres
const client = new pg.Client({
  database: 'loader_test',
  user: 'blist',
  password: ''
});

// ===== LISTEN FOR CHUNK EVENTS ============

const pgEvents = new EventEmitter();

client.connect((err, conn) => {
  if(err) throw err;
  console.log('connected');

  conn.query('LISTEN new_chunks').then(() => {
    console.log('listening to postgres for notifications on channel "new_chunks"');
    conn.on('notification', (msg) => {
      const split = msg.payload.split(',');
      const tableName = split[0];
      const chunkIdx = split[1];
      pgEvents.emit('chunk', {
        tableName,
        chunkIdx
      });
    });
  });

  conn.query('LISTEN upload_done').then(() => {
    console.log('listening to postgres for notifications on channel "upload_done"');
    conn.on('notification', (msg) => {
      pgEvents.emit('upload_done', {
        tableName: msg.payload
      });
    });
  });

  setupApp(conn);
});

// ====== LISTEN FOR HTTP ===================

function setupApp(conn) {

  // tell express how to handle requests
  app.get('/', (req, res) => {
    res.sendFile('public/index.html');
  });

  app.get('/table/:table/chunks/:chunkIdx', (req, res) => {
    // TODO: prevent SQL injection....
    const limit = req.params.limit || 1000;
    getChunk(conn, req.params.table, req.params.chunkIdx, limit, (chunk) => res.send(chunk), () => res.sendStatus(500));
  });

  app.get('/table/:table/chunks', (req, res) => {
    // TODO: basically just send whole uploads row
    // done at, etc
    getNumChunks(conn, req.params.table, (num) => res.send({ chunks: num }), (err) => res.sendStatus(500));
  });

  // =========== LISTEN FOR WEBSOCKET =======================

  io.on('connection', (socket) => {
    socket.on('table_events_subscribe', (data) => {
      const subscribedTableName = data.tableName;
      console.log('table_events_subscribe', subscribedTableName);

      var disconnected = false;

      // get base histo. save this?
      getNumChunks(conn, subscribedTableName, (num) => {
        console.log(num, 'chunks');
        if (num != null && num >= 0) {
          function go(curChunkIdx) {
            console.log('go', curChunkIdx);
            if (disconnected) return;

            getChunk(conn, subscribedTableName, curChunkIdx, 100000000, (chunk) => {
              const chunkHistos = [];
              console.log('got chunk', curChunkIdx, 'of', subscribedTableName, '. aggregating');
              chunk.forEach((row) => {
                aggregate(chunkHistos, row.data);
              });
              socket.emit('histo_chunk', {
                chunkIdx: curChunkIdx,
                histos: chunkHistos
              });
              if (curChunkIdx < num) {
                go(curChunkIdx + 1);
              }
            }, (err) => { throw err });
          }
          go(data.highestAggregateReceived);
        }
      }, (err) => { throw err });

      const sendChunk = (chunk) => {
        if (chunk.tableName === subscribedTableName) {
          getChunk(conn, subscribedTableName, chunk.chunkIdx, 100000000, (dbChunk) => {
            const chunkHistos = [];

            dbChunk.forEach((row) => {
              aggregate(chunkHistos, row.data);
            });

            console.log('sending histo chunk', chunkHistos.length, chunk.chunkIdx);
            socket.emit('histo_chunk', {
              chunkIdx: parseInt(chunk.chunkIdx),
              // histos: chunkHistos
              histos: []
            });
          }, (err) => { throw err });
        }
      };
      pgEvents.on('chunk', sendChunk);

      const sendDone = (msg) => {
        if (msg.tableName === subscribedTableName) {
          console.log('DUNZO');
          socket.emit('upload_done', {});
        }
      }
      pgEvents.on('upload_done', sendDone);

      socket.on('disconnect', () => {
        disconnected = true;
        console.log('disconnecting from table', subscribedTableName);
        pgEvents.removeListener('chunk', sendChunk);
        pgEvents.removeListener('upload_done', sendDone);
      });
    });
  });

  server.listen(4000, () => {
    console.log('listening on http://localhost:4000/');
  });

}


function getChunk(conn, tableName, chunkIdx, limit, resolve, reject) {
  conn.query(`SELECT * FROM ${tableName} WHERE chunk_idx = $1 order by row_idx limit $2`, [chunkIdx, limit], (err, queryResp) => {
    if (err) reject(err);

    resolve(queryResp.rows);
  });
}

// TODO: use pg-promise
function getNumChunks(conn, tableName, resolve, reject) {
  conn.query('SELECT latest_chunk_idx FROM uploads WHERE table_name = $1', [tableName], (err, queryResp) => {
    if (err) reject(err);

    if (queryResp.rows && queryResp.rows.length > 0) {
      resolve(queryResp.rows[0].latest_chunk_idx);
    } else {
      resolve(null);
    }
  });
}


function aggregate(currentHistos, row) {
  if (currentHistos.length === 0) {
    for (var j = 0; j < row.length; j++) {
      currentHistos.push({});
    }
  }
  for(var i = 0; i < row.length; i++) {
    updateHisto(currentHistos[i], row[i]);
  }
}

function updateHisto(histo, newValue) {
  if (histo[newValue]) {
    histo[newValue] += 1;
  } else {
    histo[newValue] = 1;
  }
}


