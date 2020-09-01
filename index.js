const Node = require('./lib/node')
const Get = require('./lib/get')
const Put = require('./lib/put')
const Batch = require('./lib/batch')
const Delete = require('./lib/del')
const History = require('./lib/history')
const Iterator = require('./lib/iterator')
const Watch = require('./lib/watch')
const Diff = require('./lib/diff')
const { Header } = require('./lib/messages')
const mutexify = require('mutexify')
const thunky = require('thunky')
const codecs = require('codecs')
const bulk = require('bulk-write-stream')
const toStream = require('nanoiterator/to-stream')
const isOptions = require('is-options')
const ddatabase = require('ddatabase')
const inherits = require('inherits')
const events = require('events')

module.exports = DWebTrie

function DWebTrie (storage, key, opts) {
  if (!(this instanceof DWebTrie)) return new DWebTrie(storage, key, opts)

  if (isOptions(key)) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  events.EventEmitter.call(this)

  this.id = null
  this.key = null
  this.discoveryKey = null
  this.secretKey = null
  this.metadata = opts.metadata || null
  this.hash = opts.hash || null
  this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
  this.alwaysUpdate = !!opts.alwaysUpdate

  const feedOpts = Object.assign({}, opts, { valueEncoding: 'binary' })
  this.feed = opts.feed || ddatabase(storage, key, feedOpts)
  this.opened = false
  this.ready = thunky(this._ready.bind(this))

  this._watchers = []
  this._checkout = (opts && opts.checkout) || 0
  this._lock = mutexify()

  if (this.feed !== opts.feed) this.feed.on('error', this._onerror.bind(this))
  if (!this._checkout) this.feed.on('append', this._onappend.bind(this))
}

inherits(DWebTrie, events.EventEmitter)

Object.defineProperty(DWebTrie.prototype, 'version', {
  enumerable: true,
  get: function () {
    return this._checkout || this.feed.length
  }
})

DWebTrie.prototype._onerror = function (err) {
  this.emit('error', err)
}

DWebTrie.prototype._onappend = function () {
  for (var i = 0; i < this._watchers.length; i++) {
    this._watchers[i].update()
  }

  this.emit('append')
}

DWebTrie.prototype._ready = function (cb) {
  const self = this

  this.feed.ready(function (err) {
    if (err) return done(err)

    if (self.feed.length || !self.feed.writable) return done(null)
    self.feed.append(Header.encode({type: 'dwebtrie', metadata: self.metadata}), done)

    function done (err) {
      if (err) return cb(err)
      if (self._checkout === -1) self._checkout = self.feed.length
      self.id = self.feed.id
      self.key = self.feed.key
      self.discoveryKey = self.feed.discoveryKey
      self.secretKey = self.feed.secretKey
      self.opened = true
      self.emit('ready')
      cb(null)
    }
  })
}

DWebTrie.prototype.getMetadata = function (cb) {
  this.feed.get(0, { valueEncoding: Header }, (err, header) => {
    if (err) return cb(err)
    return cb(null, header.metadata)
  })
}

DWebTrie.prototype.setMetadata = function (metadata) {
  // setMetadata can only be called before this.ready is first called.
  if (this.feed.length || !this.feed.writable) throw new Error('The metadata must be set before any puts have occurred.')
  this.metadata = metadata
}

DWebTrie.prototype.replicate = function (isInitiator, opts) {
  return this.feed.replicate(isInitiator, opts)
}

DWebTrie.prototype.checkout = function (version) {
  if (version === 0) version = 1
  return new DWebTrie(null, null, {
    checkout: version || 1,
    valueEncoding: this.valueEncoding,
    feed: this.feed
  })
}

DWebTrie.prototype.snapshot = function () {
  return this.checkout(this.version)
}

DWebTrie.prototype.head = function (cb) {
  const self = this

  if (!this.opened) return readyAndHead(this, cb)
  if (this._checkout !== 0) return this.getBySeq(this._checkout - 1, cb)
  if (this.alwaysUpdate) this.feed.update({ hash: false, ifAvailable: true }, onupdated)
  else process.nextTick(onupdated)

  function onupdated () {
    if (self.feed.length < 2) return cb(null, null)
    self.getBySeq(self.feed.length - 1, cb)
  }
}

DWebTrie.prototype.list = function (prefix, opts, cb) {
  if (typeof prefix === 'function') return this.list('', null, prefix)
  if (typeof opts === 'function') return this.list(prefix, null, opts)

  const ite = this.iterator(prefix, opts)
  const res = []

  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return cb(null, res)
    res.push(node)
    ite.next(loop)
  })
}

DWebTrie.prototype.iterator = function (prefix, opts) {
  if (isOptions(prefix)) return this.iterator('', prefix)
  return new Iterator(this, prefix, opts)
}

DWebTrie.prototype.createReadStream = function (prefix, opts) {
  return toStream(this.iterator(prefix, opts))
}

DWebTrie.prototype.history = function (opts) {
  return new History(this, opts)
}

DWebTrie.prototype.createHistoryStream = function (opts) {
  return toStream(this.history(opts))
}

DWebTrie.prototype.diff = function (other, prefix, opts) {
  if (Buffer.isBuffer(other)) return this.diff(0, prefix, Object.assign(opts || {}, { checkpoint: other }))
  if (isOptions(prefix)) return this.diff(other, null, prefix)
  const checkout = (typeof other === 'number' || !other) ? this.checkout(other) : other
  return new Diff(this, checkout, prefix, opts)
}

DWebTrie.prototype.createDiffStream = function (other, prefix, opts) {
  return toStream(this.diff(other, prefix, opts))
}

DWebTrie.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  return new Get(this, key, opts, cb)
}

DWebTrie.prototype.watch = function (key, onchange) {
  if (typeof key === 'function') return this.watch('', key)
  return new Watch(this, key, onchange)
}

DWebTrie.prototype.batch = function (ops, cb) {
  return new Batch(this, ops, cb || noop)
}

DWebTrie.prototype.put = function (key, value, opts, cb) {
  if (typeof opts === 'function') return this.put(key, value, null, opts)
  opts = Object.assign({}, opts, {
    batch: null,
    del: 0
  })
  return new Put(this, key, value, opts, cb || noop)
}

DWebTrie.prototype.del = function (key, opts, cb) {
  if (typeof opts === 'function') return this.del(key, null, opts)
  opts = Object.assign({}, opts, {
    batch: null
  })
  return new Delete(this, key, opts, cb)
}

DWebTrie.prototype.createWriteStream = function (opts) {
  const self = this
  return bulk.obj(write)

  function write (batch, cb) {
    if (batch.length && Array.isArray(batch[0])) batch = flatten(batch)
    self.batch(batch, cb)
  }
}

DWebTrie.prototype.getBySeq = function (seq, opts, cb) {
  if (typeof opts === 'function') return this.getBySeq(seq, null, opts)
  if (seq < 1) return process.nextTick(cb, null, null)

  const self = this
  this.feed.get(seq, opts, onnode)

  function onnode (err, val) {
    if (err) return cb(err)
    const node = Node.decode(val, seq, self.valueEncoding, self.hash)
    // early exit for the key: '' nodes we write to reset the db
    if (!node.value && !node.key) return cb(null, null)
    cb(null, node)
  }
}

function noop () {}

function readyAndHead (self, cb) {
  self.ready(function (err) {
    if (err) return cb(err)
    self.head(cb)
  })
}

function flatten (list) {
  const result = []
  for (var i = 0; i < list.length; i++) {
    const next = list[i]
    for (var j = 0; j < next.length; j++) result.push(next[j])
  }
  return result
}
