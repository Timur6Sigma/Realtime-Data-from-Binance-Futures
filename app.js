const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const { performance } = require('perf_hooks');

// #####################################################################

console.log("Program started at:");
var startTime = performance.now();
var date = new Date();
console.log(date.toGMTString());

// #####################################################################

// Open Database
let db = new sqlite3.Database("./DB.db", sqlite3.OPEN_READWRITE, (err) => {
  if (err) {
    console.log("Could not connect to SQlite database ...");
    return console.error(err.message);
  }
  console.log('Connected to SQlite database successfully ...');
});
// Create "trades" Table in Database
db.run("CREATE TABLE trade(Stream, Symbol, Trade_ID, Event_type, Trade_time, Event_time, Price, Quantity, Buyer_is_market_maker)");
console.log("Created SQL Table 'trade' ...");
db.run("CREATE TABLE aggTrade(Stream, Symbol, aggregated_trade_ID, First_trade_ID, Last_trade_ID, Event_type, Trade_time, Event_time, Price, Quantity, Buyer_is_market_maker)");
console.log("Created SQL Table 'aggTrade' ...");
db.run("CREATE TABLE depth(Stream, Symbol, Order_book_updateId, First_update_ID_in_event, Last_update_ID_in_event, Trade_time, Event_time, bidLevel20_price, bidLevel20_quantity, bidLevel19_price, bidLevel19_quantity, bidLevel18_price, bidLevel18_quantity, bidLevel17_price, bidLevel17_quantity, bidLevel16_price, bidLevel16_quantity, bidLevel15_price, bidLevel15_quantity, bidLevel14_price, bidLevel14_quantity, bidLevel13_price, bidLevel13_quantity, bidLevel12_price, bidLevel12_quantity, bidLevel11_price, bidLevel11_quantity, bidLevel10_price, bidLevel10_quantity, bidLevel9_price, bidLevel9_quantity, bidLevel8_price, bidLevel8_quantity, bidLevel7_price, bidLevel7_quantity, bidLevel6_price, bidLevel6_quantity, bidLevel5_price, bidLevel5_quantity, bidLevel4_price, bidLevel4_quantity, bidLevel3_price, bidLevel3_quantity, bidLevel2_price, bidLevel2_quantity, bidLevel1_price, bidLevel1_quantity, askLevel1_price, askLevel1_quantity, askLevel2_price, askLevel2_quantity, askLevel3_price, askLevel3_quantity, askLevel4_price, askLevel4_quantity, askLevel5_price, askLevel5_quantity, askLevel6_price, askLevel6_quantity, askLevel7_price, askLevel7_quantity, askLevel8_price, askLevel8_quantity, askLevel9_price, askLevel9_quantity, askLevel10_price, askLevel10_quantity, askLevel11_price, askLevel11_quantity, askLevel12_price, askLevel12_quantity, askLevel13_price, askLevel13_quantity, askLevel14_price, askLevel14_quantity, askLevel15_price, askLevel15_quantity, askLevel16_price, askLevel16_quantity, askLevel17_price, askLevel17_quantity, askLevel18_price, askLevel18_quantity, askLevel19_price, askLevel19_quantity, askLevel20_price, askLevel20_quantity)");
console.log("Created SQL Table 'depth' ...");
db.run("CREATE TABLE markPrice(Stream, Symbol, Event_type, Event_time, Mark_price, Index_price, Estimated_Settle_Price, Funding_rate, Next_funding_time)");
console.log("Created SQL Table 'markPrice' ...");
db.run("CREATE TABLE forceOrder(Stream, Symbol, Event_type, Event_time, Side, Order_type, Time_in_Force, Original_Quantity, Price, Average_Price, Order_Status, Order_Last_filled_quantity, Order_Filled_Accumulated_Quantity, Order_Trade_Time)");
console.log("Created SQL Table 'forceOrder' ...");

// Create SQL Command to insert retrieved data to Database
const sql_trade = "INSERT INTO trade (Stream, Symbol, Trade_ID, Event_type, Trade_time, Event_time, Price, Quantity, Buyer_is_market_maker) VALUES(?,?,?,?,?,?,?,?,?)";

const sql_aggTrade = "INSERT INTO aggTrade (Stream, Symbol, aggregated_trade_ID, First_trade_ID, Last_trade_ID, Event_type, Trade_time, Event_time, Price, Quantity, Buyer_is_market_maker) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

const sql_depth = "INSERT INTO depth (Stream, Symbol, Order_book_updateId, First_update_ID_in_event, Last_update_ID_in_event, Trade_time, Event_time, bidLevel20_price, bidLevel20_quantity, bidLevel19_price, bidLevel19_quantity, bidLevel18_price, bidLevel18_quantity, bidLevel17_price, bidLevel17_quantity, bidLevel16_price, bidLevel16_quantity, bidLevel15_price, bidLevel15_quantity, bidLevel14_price, bidLevel14_quantity, bidLevel13_price, bidLevel13_quantity, bidLevel12_price, bidLevel12_quantity, bidLevel11_price, bidLevel11_quantity, bidLevel10_price, bidLevel10_quantity, bidLevel9_price, bidLevel9_quantity, bidLevel8_price, bidLevel8_quantity, bidLevel7_price, bidLevel7_quantity, bidLevel6_price, bidLevel6_quantity, bidLevel5_price, bidLevel5_quantity, bidLevel4_price, bidLevel4_quantity, bidLevel3_price, bidLevel3_quantity, bidLevel2_price, bidLevel2_quantity, bidLevel1_price, bidLevel1_quantity, askLevel1_price, askLevel1_quantity, askLevel2_price, askLevel2_quantity, askLevel3_price, askLevel3_quantity, askLevel4_price, askLevel4_quantity, askLevel5_price, askLevel5_quantity, askLevel6_price, askLevel6_quantity, askLevel7_price, askLevel7_quantity, askLevel8_price, askLevel8_quantity, askLevel9_price, askLevel9_quantity, askLevel10_price, askLevel10_quantity, askLevel11_price, askLevel11_quantity, askLevel12_price, askLevel12_quantity, askLevel13_price, askLevel13_quantity, askLevel14_price, askLevel14_quantity, askLevel15_price, askLevel15_quantity, askLevel16_price, askLevel16_quantity, askLevel17_price, askLevel17_quantity, askLevel18_price, askLevel18_quantity, askLevel19_price, askLevel19_quantity, askLevel20_price, askLevel20_quantity) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

const sql_markPrice = "INSERT INTO markPrice (Stream, Symbol, Event_type, Event_time, Mark_price, Index_price, Estimated_Settle_Price, Funding_rate, Next_funding_time) VALUES(?,?,?,?,?,?,?,?,?)";

const sql_forceOrder = "INSERT INTO forceOrder (Stream, Symbol, Event_type, Event_time, Side, Order_type, Time_in_Force, Original_Quantity, Price, Average_Price, Order_Status, Order_Last_filled_quantity, Order_Filled_Accumulated_Quantity, Order_Trade_Time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

// #####################################################################

var intervalTime = performance.now();
const streams = ["btcusdt@trade", "btcusdt@aggTrade", "btcusdt@depth20@100ms", "btcusdt@markPrice@1s", "btcusdt@forceOrder"];

// #####################################################################

// Create and connect to Binance Websocket
let ws = new WebSocket("wss://fstream.binance.com/stream?streams="+streams.join("/"));

function responseHandler(response) {
  if (response.stream == "btcusdt@trade") {
    db.run(sql_trade, [response.stream, response.data.s, response.data.t, response.data.e, response.data.T, response.data.E, response.data.p, response.data.q, response.data.m], (err) => {
      if (err) return consol.error(err.message);
    });
  } else if (response.stream == "btcusdt@aggTrade") {
    db.run(sql_aggTrade, [response.stream, response.data.s, response.data.a, response.data.f, response.data.l, response.data.e, response.data.T, response.data.E, response.data.p, response.data.q, response.data.m], (err) => {
      if (err) return consol.error(err.message);
    });
  } else if (response.stream == "btcusdt@depth20@100ms") {
    db.run(sql_depth, [response.stream, response.data.s, response.data.U, response.data.u, response.data.T, response.data.E, response.data.b[0][0], response.data.b[0][1], response.data.b[1][0], response.data.b[1][1], response.data.b[2][0], response.data.b[2][1], response.data.b[3][0], response.data.b[3][1], response.data.b[4][0], response.data.b[4][1], response.data.b[5][0], response.data.b[5][1], response.data.b[6][0], response.data.b[6][1], response.data.b[7][0], response.data.b[7][1], response.data.b[8][0], response.data.b[8][1], response.data.b[9][0], response.data.b[9][1], response.data.b[10][0], response.data.b[10][1], response.data.b[11][0], response.data.b[11][1], response.data.b[12][0], response.data.b[12][1], response.data.b[13][0], response.data.b[13][1], response.data.b[14][0], response.data.b[14][1], response.data.b[15][0], response.data.b[15][1], response.data.b[16][0], response.data.b[16][1], response.data.b[17][0], response.data.b[17][1], response.data.b[18][0], response.data.b[18][1], response.data.b[19][0], response.data.b[19][1], response.data.a[0][0], response.data.a[0][1], response.data.a[1][0], response.data.a[1][1], response.data.a[2][0], response.data.a[2][1], response.data.a[3][0], response.data.a[3][1], response.data.a[4][0], response.data.a[4][1], response.data.a[5][0], response.data.a[5][1], response.data.a[6][0], response.data.a[6][1], response.data.a[7][0], response.data.a[7][1], response.data.a[8][0], response.data.a[8][1], response.data.a[9][0], response.data.a[9][1], response.data.a[10][0], response.data.a[10][1], response.data.a[11][0], response.data.a[11][1], response.data.a[12][0], response.data.a[12][1], response.data.a[13][0], response.data.a[13][1], response.data.a[14][0], response.data.a[14][1], response.data.a[15][0], response.data.a[15][1], response.data.a[16][0], response.data.a[16][1], response.data.a[17][0], response.data.a[17][1], response.data.a[18][0], response.data.a[18][1], response.data.a[19][0], response.data.a[19][1]], (err) => {
      if (err) return consol.error(err.message);
    });
  } else if (response.stream == "btcusdt@markPrice@1s") {
    db.run(sql_markPrice, [response.stream, response.data.s, response.data.e, response.data.E, response.data.p, response.data.i, response.data.P, response.data.r, response.data.T], (err) => {
      if (err) return consol.error(err.message);
    });
  } else if (response.stream == "btcusdt@forceOrder") {
      db.run(sql_forceOrder, [response.stream, response.data.o.s, response.data.e, response.data.E, response.data.o.s, response.data.o.o, response.data.o.f, response.data.o.q, response.data.o.p, response.data.o.ap, response.data.o.X, response.data.o.l, response.data.o.z, response.data.o.T], (err) => {
      if (err) return consol.error(err.message);
    });
  }
}

ws.onmessage = (event) => {
  let response = JSON.parse(event.data);

  if (performance.now()-intervalTime > 60000) {
    intervalTime = performance.now();
    date = new Date();
    console.log(date.toGMTString());
  }

  responseHandler(response);
}

ws.on("ping", function heartbeat() {
    date = new Date();
    console.log(date.toGMTString(), "| Got a Ping | Pong has been sent automatically");
});

ws.on("pong", function heartbeat() {
    date = new Date();
    console.log(date.toGMTString(), "| Got a Pong ...");
});
