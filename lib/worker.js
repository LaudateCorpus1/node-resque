const os = require('os')
const EventEmitter = require('events').EventEmitter
const Queue = require('./queue.js').Queue
const PluginRunner = require('./pluginRunner.js')
const util = require('util')

function prepareJobs (jobs) {
  return Object.keys(jobs).reduce(function (h, k) {
    var job = jobs[k]
    h[k] = typeof job === 'function' ? { perform: job } : job
    return h
  }, {})
}

class Worker extends EventEmitter {
  constructor (options, jobs) {
    super()
    if (!jobs) { jobs = {} }

    const defaults = {
      name: os.hostname() + ':' + process.pid, // assumes only one worker per node process
      queues: '*',
      timeout: 5000,
      blockTimeout: false,
      looping: true
    }

    for (const i in defaults) {
      if (options[i] === undefined || options[i] === null) { options[i] = defaults[i] }
    }

    this.options = options
    this.jobs = prepareJobs(jobs)
    this.name = this.options.name
    this.queues = this.options.queues
    this.error = null
    this.result = null
    this.ready = false
    this.running = false
    this.working = false
    this.job = null
    this.paused = false
    this.started = false

    this.queueObject = new Queue({ connection: options.connection }, this.jobs)
    this.queueObject.on('error', (error) => { this.emit('error', error) })

    this.queueObject.on('redis_connect', (options) => { this.emit('redis_connect', options) })
    this.queueObject.on('redis_close', (options) => { this.emit('redis_close', options) })
    this.queueObject.on('redis_reconnecting', (options) => { this.emit('redis_reconnecting', options) })
  }

  async connect () {
    await this.queueObject.connect()
    this.connection = this.queueObject.connection
    await this.checkQueues()
  }

  async start () {
    if (this.ready) {
      this.emit('start', new Date())
      await this.init()
      this.poll()
    }
  }

  async init () {
    await this.track()
  }

  async end () {
    this.running = false

    if (this.working === true && this.options.timeout) {
      await new Promise((resolve) => { setTimeout(() => { resolve() }, this.options.timeout) })
      return this.end()
    }

    if (this.connection && (
      this.connection.connected === true || this.connection.connected === undefined || this.connection.connected === null)
    ) {
      await this.untrack()
    }

    await this.queueObject.end()
    this.emit('end', new Date())
  }

  async poll (nQueue) {
    if (this.paused) {
      this.emit('sleep')

      await this.pause()

      return
    }

    if (!nQueue) { nQueue = 0 }
    if (!this.running) { return }

    this.queue = this.queues[nQueue]
    this.emit('poll', this.queue)

    if (this.queue === null || this.queue === undefined) {
      await this.checkQueues()
      await this.pause()
      return null
    }

    if (this.working === true) {
      const error = new Error('refusing to get new job, already working')
      this.emit('error', error, this.queue)
      return null
    }

    this.working = true

    try {
      let encodedJob
      if (this.options.blockTimeout) {
        // This is required for signal processing (SIGINT, SIGTERM, etc)
        await new Promise(resolve => setTimeout(resolve, 1));

        // ioredis returns promises. node-redis does not.
        const pop = util.promisify(this.connection.redis.blpop).bind(this.connection.redis)
        const popped = await pop(this.connection.key('queue', this.queue), this.options.blockTimeout / 1000)

        if (popped && popped.length > 1) {
          encodedJob = popped[1]
        }
      } else {
        const pop = util.promisify(this.connection.redis.lpop).bind(this.connection.redis)
        encodedJob = await pop(this.connection.key('queue', this.queue))
      }

      if (encodedJob) {
        const currentJob = JSON.parse(encodedJob.toString())
        if (this.options.looping) {
          this.result = null
          return this.perform(currentJob.class, currentJob)
        } else {
          return currentJob
        }
      } else {
        this.working = false
        if (nQueue === this.queues.length - 1) {
          await this.pause()
          return null
        } else {
          return this.poll(nQueue + 1)
        }
      }
    } catch (error) {
      this.emit('error', error, this.queue)
      this.working = false
      await this.pause()
      return null
    }
  }

  async perform (jobClass, job) {
    this.job = job
    this.error = null
    let toRun

    let foundJob = this.jobs[job.class]

    if (!foundJob) {
      foundJob = this.jobs['*']
    }

    if (!foundJob) {
      this.error = new Error(`No job defined for class "${job.class}"`)
      return this.completeJob(false)
    }

    const perform = foundJob.perform
    if (!perform || typeof perform !== 'function') {
      this.error = new Error(`Missing Job: "${job.class}"`)
      return this.completeJob(false)
    }

    this.emit('job', this.queue, this.job)

    let triedAfterPerform = false
    try {
      toRun = await PluginRunner.RunPlugins(this, 'beforePerform', job.class, this.queue, foundJob, job.args)
      if (toRun === false) { return this.completeJob(false) }

      let callableArgs = [job.args]
      if (job.args === undefined || (job.args instanceof Array) === true) {
        callableArgs = job.args
      }

      for (const i in callableArgs) {
        if ((typeof callableArgs[i] === 'object') && (callableArgs[i] !== null)) { Object.freeze(callableArgs[i]) }
      }

      this.result = await perform.apply(this, [this.queue, jobClass, callableArgs])
      triedAfterPerform = true
      toRun = await PluginRunner.RunPlugins(this, 'afterPerform', job.class, this.queue, foundJob, job.args)
      return this.completeJob(true)
    } catch (error) {
      this.error = error
      if (!triedAfterPerform) {
        try {
          await PluginRunner.RunPlugins(this, 'afterPerform', job.class, this.queue, foundJob, job.args)
        } catch (error) {
          if (error && !this.error) { this.error = error }
        }
      }
      return this.completeJob(!this.error)
    }
  }

  async completeJob (toRespond) {
    if (this.error) {
      await this.fail(this.error)
    } else if (toRespond) {
      await this.succeed(this.job)
    }

    this.working = false
    this.job = null

    if (this.options.looping) {
      this.poll()
    }
  }

  async succeed (job) {
    this.emit('success', this.queue, job, this.result)
  }

  async fail (err) {
    this.emit('failure', this.queue, this.job, err)
  }

  setPause () {
    this.emit('paused')

    this.paused = true
  }

  resume () {
    this.emit('resume')

    this.paused = false
  }

  async pause () {
    this.emit('pause')

    this.queues = this.shuffle(this.queues)

    await new Promise((resolve) => {
      if (this.options.blockTimeout) {
        this.poll()
        resolve()
      } else {
        setTimeout(() => {
          this.poll()
          resolve()
        }, this.options.timeout)
      }
    })
  }

  async track () {
    this.running = true
  }

  async untrack () {
    return
  }

  // https://stackoverflow.com/a/12646864/1671573
  shuffle (inArr) {
    for (let i = inArr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [inArr[i], inArr[j]] = [inArr[j], inArr[i]];
    }

    return inArr
  }

  async checkQueues () {
    if (typeof this.queues === 'string') {
      this.queues = this.queues.split(',')
      this.ready = true
    }

    if (Array.isArray(this.queues) && this.queues.length > 0) {
      this.ready = true
    }

    if ((this.queues[0] === '*' && this.queues.length === 1) || this.queues.length === 0) {
      this.originalQueue = '*'
      await this.untrack()
      const response = await this.connection.redis.smembers(this.connection.key('queues'))
      this.queues = (response ? response.sort() : [])
      await this.track()
    }

    this.shuffle(this.queues)
  }

  stringQueues () {
    if (this.queues.length === 0) {
      return ['*'].join(',')
    } else {
      try {
        return this.queues.join(',')
      } catch (e) {
        return ''
      }
    }
  }
}

exports.Worker = Worker
