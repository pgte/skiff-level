'use strict';

var async = require('async');
var level = require('level');
var sublevel = require('level-sublevel');
var inherits = require('util').inherits;
var LevelWriteStream = require('level-writestream');
var SkiffPersistence = require('abstract-skiff-persistence');

module.exports = SkiffLevel;

var options = {
  valueEncoding: 'json'
};

function SkiffLevel(path) {
  this.db = sublevel(level(path, options));
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
      commitIndex: main.sublevel('commitIndex')
    };

    LevelWriteStream(node.state);
  }

  return node;
};

SL._saveMeta = function _saveMeta(nodeId, state, callback) {
  this._nodeSublevels(nodeId).meta.put(nodeId, state, callback);
};

SL._loadMeta = function _loadMeta(nodeId, callback) {
  this._nodeSublevels(nodeId).meta.get(nodeId, callback);
};

SL._applyCommand =
function _applyCommand(nodeId, commitIndex, command, callback) {
  var nodeSublevels = this._nodeSublevels(nodeId);
  this.db.batch([
    {
      key: command.key,
      value: command.value,
      type: command.type,
      prefix: nodeSublevels.state
    },
    {
      key: nodeId,
      value: commitIndex,
      type: 'put',
      prefix: nodeSublevels.commitIndex
    }
  ], callback);
};

SL._lastAppliedCommitIndex =
function _lastAppliedCommitIndex(nodeId, callback) {
  this._nodeSublevels(nodeId).commitIndex.get(nodeId, callback);
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