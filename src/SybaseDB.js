var spawn = require('child_process').spawn;
var JSONStream = require('JSONStream');
var fs = require("fs");
var path = require("path");

function Sybase(logTiming, pathToJavaBridge, { encoding = "utf8", extraLogs = false } = {}) {
    this.connected = false;
    this.logTiming = (logTiming == true);
    this.encoding = encoding;
    this.extraLogs = extraLogs;

    this.pathToJavaBridge = pathToJavaBridge;
    if (this.pathToJavaBridge === undefined) {
        this.pathToJavaBridge = path.resolve(__dirname, "..", "JavaSybaseLink", "dist", "JavaSybaseLink.jar");
    }

    this.queryCount = 0;
    this.currentMessages = {}; // look up msgId to message sent and call back details.

    this.jsonParser = JSONStream.parse();

    this.javaDB = spawn('java', ["-jar", this.pathToJavaBridge]);
    var that = this;
    // set up normal listeners.
    that.javaDB.stdout.setEncoding(that.encoding).pipe(that.jsonParser).on("data", function (jsonMsg) { that.onSQLResponse.call(that, jsonMsg); });
    that.javaDB.stderr.on("data", function (err) { that.onSQLError.call(that, err); });
}

Sybase.prototype.log = function (msg) {
    if (this.extraLogs) {
        console.log(msg);
    }
}

Sybase.prototype.connect = function (host, port, dbname, username, password, charset, timezone, callback) {
    var hrstart = process.hrtime();
    this.queryCount++;
    var msg = {};
    msg.type = "connect";
    msg.msgId = this.queryCount;
    msg.host = host;
    msg.port = port;
    msg.dbname = dbname;
    msg.username = username;
    msg.password = password;
    msg.charset = charset;
    msg.timezone = timezone;
    msg.sentTime = (new Date()).getTime();
    var strMsg = JSON.stringify(msg).replace(/[\n]/g, '\\n');
    msg.callback = callback;
    msg.hrstart = hrstart;

    this.log("this: " + this + " currentMessages: " + this.currentMessages + " this.queryCount: " + this.queryCount);

    this.currentMessages[msg.msgId] = msg;

    this.javaDB.stdin.write(strMsg + "\n");
    this.log("sql request written: " + strMsg);
    this.connected = true;
};

Sybase.prototype.close = function (callback) {
    var hrstart = process.hrtime();
    this.queryCount++;
    var msg = {};
    msg.type = "close";
    msg.msgId = this.queryCount;
    msg.sentTime = (new Date()).getTime();
    var strMsg = JSON.stringify(msg).replace(/[\n]/g, '\\n');
    msg.callback = callback;
    msg.hrstart = hrstart;

    this.log("this: " + this + " currentMessages: " + this.currentMessages + " this.queryCount: " + this.queryCount);

    this.currentMessages[msg.msgId] = msg;

    this.javaDB.stdin.write(strMsg + "\n");
    this.log("sql request written: " + strMsg);
    this.connected = false;
}

Sybase.prototype.isConnected = function () {
    return this.connected;
};

Sybase.prototype.query = function (sql, callback) {
    if (this.isConnected() === false) {
        callback(new Error("database isn't connected."));
        return;
    }
    var hrstart = process.hrtime();
    this.queryCount++;
    var msg = {};
    msg.type = "query";
    msg.msgId = this.queryCount;
    msg.sql = sql;
    msg.sentTime = (new Date()).getTime();
    var strMsg = JSON.stringify(msg).replace(/[\n]/g, '\\n');
    msg.callback = callback;
    msg.hrstart = hrstart;

    this.log("this: " + this + " currentMessages: " + this.currentMessages + " this.queryCount: " + this.queryCount);

    this.currentMessages[msg.msgId] = msg;

    this.javaDB.stdin.write(strMsg + "\n");
    this.log("sql request written: " + strMsg);
};

Sybase.prototype.onSQLResponse = function (jsonMsg) {
    var err = null;
    var request = this.currentMessages[jsonMsg.msgId];
    delete this.currentMessages[jsonMsg.msgId];

    var result = jsonMsg.result;
    if (result.length === 1)
        result = result[0]; //if there is only one just return the first RS not a set of RS's

    var currentTime = (new Date()).getTime();
    var sendTimeMS = currentTime - jsonMsg.javaEndTime;
    hrend = process.hrtime(request.hrstart);
    var javaDuration = (jsonMsg.javaEndTime - jsonMsg.javaStartTime);

    if (jsonMsg.error !== undefined)
        err = new Error(jsonMsg.error);


    if (this.logTiming)
        console.log("Execution time (hr): %ds %dms dbTime: %dms dbSendTime: %d sql=%s", hrend[0], hrend[1] / 1000000, javaDuration, sendTimeMS, request.sql);
    request.callback(err, result);
};

Sybase.prototype.onSQLError = function (data) {
    var error = new Error(data);

    var callBackFuncitons = [];
    for (var k in this.currentMessages) {
        if (this.currentMessages.hasOwnProperty(k)) {
            callBackFuncitons.push(this.currentMessages[k].callback);
        }
    }

    // clear the current messages before calling back with the error.
    this.currentMessages = [];
    callBackFuncitons.forEach(function (cb) {
        cb(error);
    });
};

module.exports = Sybase;
