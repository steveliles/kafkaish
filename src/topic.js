import { EventEmitter } from 'events'

const SUBSCRIBER_COLLECTION_OPTS = {}

class Topic extends EventEmitter {
  constructor(connection, name, opts={}){
    super()
    opts.capped = true
    opts.size = opts.size || (opts.size = 1024 * 1024 * 5);
    opts.strict = false;

    this.opts = opts;
    this.connection = connection;
    this.closed = false;
    this.listening = null;
    this.name = name || 'kafkaesque';

    this.setMaxListeners(0);

    process.nextTick(this.create.bind(this))
  }
  publish(event, message, callback){
    this.collection.insert({
      event: event,
      message: message
    }, (err, docs) => {
      if (callback) {
        if (err) {
          callback(err)
        } else {
          callback(null, docs.ops[0]);
        }
      }
    })
  }
  subscribe(event, opts, callback){
    if (opts.name && opts.replay){
      return this.replay(event, opts, callback)
    } else {
      return this.join(event, opts, callback)
    }
  }
  listen(){
    this.latest(null, (err, doc)=>{
      const cursor = this.collection
        .find({
          _id: {
            $gt: doc._id
          }
        })
      cursor.addCursorFlag('tailable',true)
      cursor.addCursorFlag('awaitData',true)
      const next = (doc) => {
        if (!doc) {
          // todo: maybe if collection is dropped or cursor closed for some external
          // reason we'll see no doc here but it isn't the end of the cursor?
          console.log('no doc, what happened?')
        } else {
          this.emit(doc.event, doc.message)
          process.nextTick(more)
        }
      }
      const more = () => {
        cursor.nextObject(next);
      }
      this.emit('ready')
      process.nextTick(more)
    })
  }
  join(event, opts, callback){
    if (opts.name){
      // durable subscription so the subscriber needs to acknowledge
      // each message as having been processed successfully
      this.on(event, (msg)=>{
        callback(msg, ()=>{
          this.ack(opts.name,msg._id)
        })
      })
    } else {
      // subscriber doesn't want durable subscription, so no
      // don't bother with the acknowledgement callback
      this.on(event, (msg)=>{
        callback(msg)
      })
    }
    return {
      unsubscribe: () => {
        this.removeListener(event, callback)
      }
    }
  }
  replay(event, opts, callback){
    // find consumer's last ack'd position
    subscribers.findOne({name: opts.name}, (err,info)=>{
      if (err) {
        this.emit('error',err)
      } else {
        if (info) {
          // existing subscriber, replay from last ack
          return this.replayFrom(info.last, event, opts, callback)
        } else {
          // new subscriber, replay from the beginning
          return this.replayFrom(null, event, opts, callback)
        }
      }
    })
  }
  replayFrom(last, event, opts, callback){
    this.latest(last, (err, doc)=>{
      const cursor = this.collection
        .find({
          _id: {
            $gt: doc._id
          }
        })
      let subscribed = true
        , subscription = {
          unsubscribe: () => {
            subscribed = false
          }
        }
      const next = (doc) => {
        // todo: maybe if collection is dropped or cursor closed for some external
        // reason we'll see no doc here but it isn't the end of the cursor?
        if (!doc) {
          subscription.unsubscribe = this.join(event, opts, callback).unsubscribe
          cursor.close()
        } else {
          if (!event || (doc.event === event)) {
            callback(msg, ()=>{
              this.ack(opts.name,msg._id)
              if (subscribed) {
                process.nextTick(more)
              }
            })
          } else {
            process.nextTick(more)
          }
        }
      }
      const more = () => {
        cursor.nextObject(next);
      }
      process.nextTick(more)
      return subscription
    })
  }
  ack(name,id){
    // todo - write the id as the last known position of the given subscriber
  }
  latest(latest, callback){
    const collection = this.collection
    collection
      .find(latest ? { _id: latest } : null, {timeout: false})
      .limit(1)
      .nextObject((err, doc) => {
        if (err || doc) {
          return callback(err, doc, collection)
        } else {
          collection.insert({ 'dummy': true }, { safe: true }, (err, docs) => {
            if (err) {
              callback(err)
            } else {
              callback(err, docs.ops[0], collection)
            }
          })
        }
      })
  }
  ensureCollection(name,opts,callback) {
    this.connection.db.collections((err, collections)=>{
      if (collections.find(c => c.collectionName === name)) {
        callback(null, true)
      } else {
        this.connection.db.createCollection(
          name,
          opts,
          callback
        )
      }
    })
  }
  create() {
    if (!this.connection.db) {
      this.emit('not-connected')
    } else {
      this.ensureCollection(this.name, this.opts, (cerr,collection)=>{
        if (cerr) {
          this.emit('error',cerr)
        } else {
          this.collection = collection
          this.ensureCollection(`${this.name}_subscribers`, SUBSCRIBER_COLLECTION_OPTS, (serr,subscribers)=>{
            if (serr){
              this.emit('error',serr)
            } else {
              this.subscribers = subscribers
              this.listen()
            }
          })
        }
      })
    }
    return this
  }
}

export default Topic
