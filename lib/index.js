'use strict';

var async = require('async');
var level = require('level');
var concat = require('concat-stream');
var msgpack = require('msgpack5')();
var sublevel = require('level-sublevel');
var inherits = require('util').inherits;
var LevelWriteStream = require('level-writestream');
var SkiffPersistence = require('abstract-skiff-persistence');

module.exports = SkiffLevel;

var options = {
  valueEncoding: {
    encode: encode,
    decode: decode,
    buffer: true,
    type: 'msgpack5'
  }
};

function encode(val) {
  return msgpack.encode(val).slice();
}

function decode(buf) {
  return msgpack.decode(buf);
}

function SkiffLevel(path) {
  SkiffPersistence.call(this);

  var self = this;

  this._main = level(path, options);

  this._main.on('error', function(err) {
    self.emit('error', err);
  });

  this.db = sublevel(this._main);
  this.nodes = {};
}

inherits(SkiffLevel, SkiffPersistence);

var SL = SkiffLevel.prototype;

SL._nodeSublevels = function _nodeSublevels(nodeId) {
  var node = this.nodes[nodeId];
  if (!node) {
    var main = this.db.sublevel(nodeId);

    node = this.nodes[nodeId] = {
      main: main,
      state: main.sublevel('state'),
      meta: main.sublevel('meta'),
      logs: main.sublevel('logs'),
      commitIndex: main.sublevel('commitIndex')
    };

    node.state.setMaxListeners(Number.MAX_VALUE);

    LevelWriteStream(node.state);
  }

  return node;
};

SL._saveMeta = function _saveMeta(nodeId, state, callback) {
  var logEntries = state.log.entries;
  delete state.log.entries;

  var nodeLevel = this._nodeSublevels(nodeId);

  var batch = [
    {
      type: 'put',
      key: nodeId,
      value: state,
      prefix: nodeLevel.meta
    }
  ];

  var minLogIndex = logEntries.length && logEntries[0].index || 0;
  var maxLogIndex = logEntries.length &&
                    logEntries[logEntries.length - 1].index || Infinity;

  var logSublevel = nodeLevel.logs;
  var logStream = logSublevel.createValueStream();
  var maxReadIndex = 0;
  logStream.on('data', function(entry) {

    var correspondingNewEntry = logEntries[entry.index - minLogIndex];
    maxReadIndex = entry.index;

    if (entry.index < minLogIndex || entry.index > maxLogIndex) {
      batch.push({
        type: 'del',
        key: entry.index,
        prefix: logSublevel
      });
    }
    else if (correspondingNewEntry &&
             correspondingNewEntry.uuid != entry.uuid) {

      batch.push({
        type: 'put',
        key: correspondingNewEntry.index,
        value: correspondingNewEntry,
        prefix: logSublevel
      });
    }
  });

  logStream.once('end', function() {
    if (maxReadIndex < maxLogIndex) {
      batch = batch.concat(
        logEntries.slice(maxReadIndex - minLogIndex + 1).map(function(entry) {
          return {
            type: 'put',
            key: entry.index,
            value: entry,
            prefix: logSublevel
          };
        })
      );
    }

    nodeLevel.main.batch(batch, callback);
  });
};

SL._loadMeta = function _loadMeta(nodeId, callback) {
  var node = this._nodeSublevels(nodeId);
  node.logs.createValueStream().pipe(concat(function(logEntries) {
    node.meta.get(nodeId, processError(function(err, meta) {
      if (err) {
        callback(err);
      } else {
        if (meta) {
          meta.log.entries = logEntries;
        }
        callback(null, meta);
      }
    }));
  }));
};

SL._applyCommand =
function _applyCommand(nodeId, commitIndex, command, callback) {
  var nodeSublevels = this._nodeSublevels(nodeId);
  var ops;

  if (command.type == 'batch' && command.operations) {
    ops = Array.prototype.slice.call(command.operations).
                map(applyToStateSublevel);
  } else {
    ops = [{
      key: command.key,
      value: command.value,
      type: command.type,
      prefix: nodeSublevels.state,
    }];
  }

  ops.push({
    key: nodeId,
    value: commitIndex,
    type: 'put',
    prefix: nodeSublevels.commitIndex
  });

  this.db.batch(ops, callback);

  function applyToStateSublevel(op) {
    return {
      key: op.key,
      value: op.value,
      type: op.type,
      prefix: nodeSublevels.state
    };
  }
};

SL._lastAppliedCommitIndex =
function _lastAppliedCommitIndex(nodeId, callback) {
  this._nodeSublevels(nodeId).commitIndex.get(nodeId, processError(callback));
};

SL._saveCommitIndex =
function _saveCommitIndex(nodeId, commitIndex, callback) {
  this._nodeSublevels(nodeId).commitIndex.put(nodeId, commitIndex, callback);
};

SL._createReadStream = function _createReadStream(nodeId) {
  return this._nodeSublevels(nodeId).state.createReadStream();
};

SL._createWriteStream = function _createWriteStream(nodeId, options) {
  return this._nodeSublevels(nodeId).state.createWriteStream(options);
};

SL._removeAllState = function _removeAllState(nodeId, cb) {
  var state = this._nodeSublevels(nodeId).state;
  var q = async.queue(removeKey, 1);
  var calledback = false;

  function removeKey(key, cb) {
    state.del(key, function(err) {
      if (err) {
        rs.destroy();
        callback(err);
      } else {
        cb();
      }
    });
  }

  var rs = this._nodeSublevels(nodeId).state.
             createReadStream({keys: true, values: false});

  rs.on('data', function(key) {
    q.push(key);
  });

  rs.once('end', function() {
    if (q.idle()) {
      callback();
    } else {
      q.drain = callback;
    }
  });

  function callback(err) {
    if (!calledback) {
      calledback = true;
      cb(err);
    }
  }
};

SL._close = function _close(cb) {
  this._main.close(cb);
};

function processError(cb) {
  return function(err, result) {
    if (err && err.notFound) {
      cb(null, undefined);
    } else {
      cb(err, result);
    }
  };
}
