const EventEmitter = require('events').EventEmitter

class Connection extends EventEmitter {
  constructor (options) {
    super()

    const defaults = {
      pkg: 'ioredis',
      host: '127.0.0.1',
      port: 6379,
      database: 0,
      namespace: 'resque',
      options: {}
    }

    if (!options) { options = {} }
    for (const i in defaults) {
      if (options[i] === null || options[i] === undefined) {
        options[i] = defaults[i]
      }
    }

    this.options = options
    this.listeners = {}
    this.connected = false
  }

  async connect () {
    if (this.options.redis) {
      this.redis = this.options.redis
    } else {
      if (this.options.pkg === 'ioredis') {
        const Pkg = require('ioredis')
        this.options.options.db = this.options.database
        if (typeof this.options.dsn === 'string') {
          this.redis = new Pkg(this.options.dsn, this.options.options)
        } else {
          this.redis = new Pkg(this.options.host, this.options.port, this.options.options)
        }
      } else {
        const Pkg = require(this.options.pkg)
        this.redis = Pkg.createClient(this.options.port, this.options.host, this.options.options)
      }
    }

    this.listeners.error = (error) => { this.emit('error', error) }
    this.redis.on('error', this.listeners.error)

    this.listeners.end = () => { this.connected = false }
    this.redis.on('end', this.listeners.end)

    this.listeners.connect = () => { this.emit('redis_connect', this.options) }
    this.redis.on('connect', this.listeners.connect)

    this.listeners.close = () => { this.emit('redis_close', this.options) }
    this.redis.on('close', this.listeners.close)

    this.listeners.reconnecting = () => { this.emit('redis_reconnecting', this.options) }
    this.redis.on('reconnecting', this.listeners.reconnecting)
  }

  end () {
    Object.keys(this.listeners).forEach((eventName) => {
      this.redis.removeListener(eventName, this.listeners[eventName])
    })

    // Only disconnect if we established the redis connection on our own.
    if (!this.options.redis && this.connected) {
      if (typeof this.redis.disconnect === 'function') { this.redis.disconnect() }
      if (typeof this.redis.quit === 'function') { this.redis.quit() }
    }

    this.connected = false
  }

  key () {
    let args
    args = (arguments.length >= 1 ? [].slice.call(arguments, 0) : [])
    if (Array.isArray(this.options.namespace)) {
      args.unshift(...this.options.namespace)
    } else {
      args.unshift(this.options.namespace)
    }
    args = args.filter((e) => { return String(e).trim() })
    return args.join(':')
  }
}

exports.Connection = Connection
