# skiff-level

LevelDB persistence provider for Skiff

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