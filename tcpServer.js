var net = require('net')
  , carrier = require('carrier')
  , Datastore = require('nedb')
  , _ = require('underscore')
  , async = require('async')
  , MongoClient = require('mongodb').MongoClient;

var accInfo = {};
var allAccounts = [];
var minuteBuffer = {};
var tickCnt = 0;
var tickTimeOut = 300; // default 300s (5 minutes) for second will be remove from memDB
var SchedulerInterval = 3 // 1 sec

MongoClient.connect(process.env.MONGO_ENV, function(err, db) {
  if(err) throw err;

  db.collection('accInfo').find().toArray(function(err, results) {
    allAccounts = results;
    _.each(results, function(item) {
      console.log(item);
      accInfo[item.heroKey] = _.clone(item);
      accInfo[item.heroKey].memdb = new Datastore();
      accInfo[item.heroKey].collection = db.collection(item.heroKey);
    });
  });
})

// receive and parse the data(line) from Heroku,
// and then put them into memDB by timestamp(ts) as second

// 129 <45>1 2013-10-17T22:42:36.286887+00:00 heroku web.26 - - Process running mem=512M(100.1%)
// 130 <45>1 2013-10-17T22:42:36.287263+00:00 heroku web.26 - - Error R14 (Memory quota exceeded)
// 129 <45>1 2013-10-17T22:42:56.111696+00:00 heroku web.26 - - Process running mem=512M(100.1%)
// 291 <158>1 2013-10-17T22:37:41.903160+00:00 heroku router - - at=error code=H12 desc="Request timeout" method=GET path=/api/v1/artists?p=2&ps=48 host=www.redbullsoundselect.com fwd="198.22.138.52" dyno=web.26 connect=9ms service=30001ms status=503 bytes=0

  /*
MongoHQ

accInfo =
   {
   _id: ObjectId("5258eb12fbc2722c0e00015c"),
   heroKey: "d.e286334a-c080-4b5c-90ae-e1a771408904",
   ibotNum: 100,
   respLimit: 600
   }

*/

var server = net.createServer(function(conn) {
  var my_carrier = carrier.carry(conn);

  my_carrier.on('line',  function(line) {
    var logType = line.substr(0, 16).split(' ')[1];

    switch (logType){
      case "<158>1":
        var vals = line.split(' ');
        var parseVal = function(val, cutoff) {
          return parseInt(val.substring(cutoff,val.length), 10);
        }
//        console.log('%s %s %s %s %s', vals[3], vals[2], vals[vals.length-3], vals[vals.length-2], vals[vals.length-1]);
        if (vals[vals.length-2].substr(7) === '200') {
          var ts = Math.floor(new Date(vals[2]).getTime()/1000)
//        console.log('%d~%d~%s~%s', accInfo[vals[3]].ibotNum , ts, parseVal(vals[vals.length-3],8), parseVal(vals[vals.length-1],6));
          accInfo[vals[3]].memdb.update({ ts: ts },
            { $inc: {
                cnt: 1,
                ms: parseVal(vals[vals.length-3],8),
                Kbyte: parseVal(vals[vals.length-1],6)/1024,
                _cnt: 1,
                _ms: parseVal(vals[vals.length-3],8),
                _byte: parseVal(vals[vals.length-1],6)
              },
              $max: {
                KbyteHigh : parseVal(vals[vals.length-1],6)/1024,
                msHigh : parseVal(vals[vals.length-3],8)
              },
              $min: {
                KbyteLow : parseVal(vals[vals.length-1],6)/1024,
                msLow : parseVal(vals[vals.length-3],8)
              }
            },
            { upsert: true },
            function(err, numReplaced, upsert) {
              (err) ? console.error(err) : tickCnt++;
            }
          );
        }

        break;
    }


  });
});
server.listen(1415);

// Repeat this process forever by interval value (SchedulerInterval)
setInterval(function(){
  var truncateSec = Math.floor(new Date().getTime()/1000) - tickTimeOut;

  async.waterfall([
    function(cb) {
      (tickCnt > 0) ? cb() : cb('skip!');
    },
    function(cb) {
      // Update/Insert all the tick data into MongoDB(item.collection) from memDB(item.memdb)
      async.map(_.values(accInfo), function(item, callback) {
        item.memdb.find({cnt : { $gt : 0}}, function (err, docs) {
          async.eachSeries(docs, function(row, innercallback) {

            var thisMin = new Date(Math.floor(row.ts/60)*60*1000).getMinutes();
            // remove the old minuteBuffer
            if (typeof minuteBuffer[(thisMin+30)%60] !== 'undefined') delete minuteBuffer[(thisMin+30)%60];

            // create the minuteBuffer for max and min values
            if (typeof minuteBuffer[thisMin] === 'undefined') {
              minuteBuffer[thisMin] = {
                KbyteHigh: row.KbyteHigh,
                KbyteLow: row.KbyteLow,
                msHigh: row.msHigh,
                msLow: row.msLow
              };
            } else {
              if (row.KbyteHigh > minuteBuffer[thisMin].KbyteHigh) minuteBuffer[thisMin].KbyteHigh = row.KbyteHigh;
              if (row.KbyteLow < minuteBuffer[thisMin].KbyteLow) minuteBuffer[thisMin].KbyteLow = row.KbyteLow;
              if (row.msHigh > minuteBuffer[thisMin].msHigh) minuteBuffer[thisMin].msHigh = row.msHigh;
              if (row.msLow < minuteBuffer[thisMin].msLow) minuteBuffer[thisMin].msLow = row.msLow;
            }

            // convert the second to minutes when update/insert into MongoDB
            item.collection.update({_id: Math.floor(row.ts/60)*60*1000 },
              { $inc: {
                  cnt: row.cnt,
                  ms: row.ms,
                  Kbyte: row.Kbyte
                },
                $set: {
                  KbyteHigh: minuteBuffer[thisMin].KbyteHigh,
                  KbyteLow: minuteBuffer[thisMin].KbyteLow,
                  msHigh: minuteBuffer[thisMin].msHigh,
                  msLow: minuteBuffer[thisMin].msLow
                }
              },
              { upsert: true, multi:false},
              function(err) {
                if (err) {
                  console.warn(err.message);
                  innercallback();
                }
                else { // remove it from the memDB
                  item.memdb.update({ ts: row.ts },
                    { $dec: { cnt: row.cnt, ms: row.ms, Kbyte: row.Kbyte } },
                    { upsert: false },
                    function(err, numReplaced, upsert) {
                      (err) ? console.error(err) : tickCnt = tickCnt - row.cnt;
                      innercallback();
                    }
                  );
                }
              });
          }, function(err, results){
            callback();
          });
        });
      }, function(err, results){
        cb();
      });
    },
    function(cb) {
      // 1/10 of the repeating processes will execute this routine
      ((truncateSec%(SchedulerInterval*10)) === 0) ? cb() : cb('skip!');
    },
    function(cb) {
      // This process will freeup the mmeDB if the ts over($lt) 5 minutes ( truncateSec = current time - tickTimeOut)
      async.map(_.values(accInfo), function(item, callback) {
        item.memdb.find({ $and : [{cnt : { $gt : 0}},{"ts": {$lt: truncateSec}}] }, function (err, docs) {
          if (err) {
            console.error(err);
            callback();
          } else {
            if (docs.length) console.log('dirty rows: %s at %s',docs.length, truncateSec);
            item.memdb.remove({"ts": {$lt: truncateSec}},
              { multi: true },
              function (err, numRemoved) {
                console.log('rows removed: %s at %s',numRemoved, truncateSec);
                callback();
              });
          }
        });
      }, function(err, results){
        cb();
      });
    }
  ],
  function(err, results) {
    if (err && !(err === 'skip!')) console.warn(err);
  });

}, SchedulerInterval * 1000);

