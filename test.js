'use strict';

var path = require('path');
var test = require('abstract-skiff-persistence/test/all');

var SkiffLevel = require('./');
var path = path.join(__dirname, 'temp', 'db');
require('rimraf').sync(path);
require('mkdirp').sync(path);
var p = new SkiffLevel(path);

var options = {
  commands: [
    {type: 'put', key: 'a', value: 1},
    {type: 'put', key: 'b', value: 2},
    {type: 'del', key: 'a'},
    {type: 'put', key: 'c', value: {some: 'object'}}
  ],
  expectedReads: [
    {key: 'b', value: 2},
    {key: 'c', value: {some: 'object'}}
  ],
  newWrites: [
    {key: 'd', value: 3},
    {key: 'e', value: {some: 'other object'}},
    {key: 'f', value: 'some other string'}
  ],
  newReads: [
    {key: 'd', value: 3},
    {key: 'e', value: {some: 'other object'}},
    {key: 'f', value: 'some other string'}
  ]
};

test(p, options);