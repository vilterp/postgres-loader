const pg = require('pg');
const _ = require('lodash');

const client = new pg.Client({
  database: 'loader_test',
  user: 'blist',
  password: ''
});

client.connect((err) => {
  if(err) throw err;
  console.log('connected');

  loadData();
});

function loadData() {
  console.error('starting', new Date());

  var i = 0;
  const query = client.query('select data from crimes');
  var histos = [];
  query.on('row', (row) => {
    aggregate(histos, row.data);
    i++;
    if (i % 1000 === 0) {
      console.error(i);
    }
  });
  query.on('end', () => {
    setTimeout(() => {
      process.exit(0);
    }, 10);
    console.error('dumping histos', new Date());
    histos.forEach((histo, i) => {
      console.log('histogram', i);
      _.keys(histo).forEach((key) => {
        console.log(key, ':', histo[key]);
      });
    })
  })
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

