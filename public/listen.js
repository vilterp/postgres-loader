console.log('hello world');

const tableName = 'crimes';
var gotFirstChunk = false;
var highestAggregateReceived = 0;
var chunksLoaded = 0;
var done = false;

tryGettingFirstChunk();

const socket = io('http://localhost:4000');
socket.on('connect', () => {
  console.log('connect')
  join();
});

function join() {
  socket.emit('table_events_subscribe', {
    tableName: tableName,
    highestAggregateReceived: highestAggregateReceived
  });
}

socket.on('histo_chunk', (chunk) => {
  console.log('histo_chunk', chunk);
  highestAggregateReceived = chunk.chunkIdx;
  if (highestAggregateReceived > chunksLoaded) {
    chunksLoaded = highestAggregateReceived;
  }
  showNumRows();

  if (chunk.chunkIdx >= 1 && !gotFirstChunk) {
    fetchAndShowFirstChunk();
  }
});

socket.on('upload_done', () => {
  console.log('done');
  done = true;
  showDone();
});

// TODO: resubscribe on new conn

function mkTd(contents) {
  const td = document.createElement('td');
  td.innerText = contents;
  return td;
}

function fetchAndShowFirstChunk() {
  fetch(`/table/${tableName}/chunks/0`).then((resp) => {
    gotFirstChunk = true;
    return resp.json().then((json) => {
      const table = document.getElementById('table');
      json.forEach((dataRow) => {
        const row = document.createElement('tr');
        row.appendChild(mkTd(dataRow.row_idx.toString()));
        dataRow.data.forEach((cell) => {
          row.appendChild(mkTd(cell));
        });
        table.appendChild(row);
      });
    }); 
  });
}


function tryGettingFirstChunk() {
  fetch(`/table/${tableName}/chunks`).then((resp) => {
    if (resp.status === 200) {
      resp.json().then((num) => {
        if (num.chunks >= 1) {
          fetchAndShowFirstChunk();
          chunksLoaded = num.chunks;
          showNumRows();
        }
      });
    }
  });
}

function showNumRows() {
  document.getElementById('num_rows').innerText = `${chunksLoaded * 10000} rows loaded`;
}

function showDone() {
  document.getElementById('done').innerText = 'Upload done!';
}
