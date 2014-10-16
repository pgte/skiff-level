# skiff-level

LevelDB persistence provider for [Skiff](https://github.com/pgte/skiff).

[![Dependency Status](https://david-dm.org/pgte/skiff-level.svg)](https://david-dm.org/pgte/skiff-level)
[![Build Status](https://travis-ci.org/pgte/skiff-level.svg)](https://travis-ci.org/pgte/skiff-level)

## Install

```bash
$ npm install skiff-level
```

## Use

```javascript
var SkiffLevel = require('skiff-level');
var Node = require('skiff');

var db = new SkiffLevel('/path/to/level/dir');

var node = Node({
  persistence: db
  // ...
});
```

# License

ISC