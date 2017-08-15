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
    this.name = name || 'kafkaish';

    this.setMaxListeners(0);
  }
  close() {
    this.listening = false
    if (this.cursor)
      this.cursor.close()
  }
  publish(event, message, callback){
    this.collection.insert({
      event: event,
      message: message
    }, {
      safe: true
    }, (err, docs) => {
      if (callback){
        if (err) {
          callback(err)
        } else {
          callback(err,docs.ops[0]);
        }
      }
    })
  }
  subscribe(event, opts, callback){
    if (opts.name && opts.replay){
      return this.replay(event, opts, callback)
    } else {
      return this.join(null, event, opts, callback)
    }
  }
  listen(since){
    if (this.listening)
      return
    this.listening = true
    this.latest(since, (err, latest)=>{
      this.cursor = this.collection
        .find({
          _id: {
            $gt: latest._id
          }
        })
      this.cursor.addCursorFlag('tailable',true)
      this.cursor.addCursorFlag('awaitData',true)
      this.cursor.addCursorFlag('noCursorTimeout',true)
      const next = (err,doc) => {
        if (err) {
          if (this.listening)
            this.emit('error',err)
          // todo: maybe need to try to recover - e.g. reconnect
        } else {
          if (!doc) {
            // todo: maybe if collection is dropped or cursor closed for some external
            // reason we'll see no doc here but it isn't the end of the cursor?
            this.emit('warn','no document')
          } else {
            this.emit(doc.event, doc)
            this.emit('message', doc);
            setImmediate(more)
          }
        }
      }
      const more = () => {
        try {
          this.cursor.nextObject(next);
        } catch (err) {
          if (this.listening)
            console.error(err)
        }
      }
      setImmediate(more)
    })
  }
  join(since, event, opts, callback){
    this.listen(since)
    let cb = undefined
    if (opts.name){
      // durable subscription so the subscriber needs to acknowledge
      // each message as having been processed successfully
      cb = (doc) => {
        callback(event, doc.message, (done)=>{
          this.ack(opts.name,doc._id,done)
        })
      }
    } else {
      // subscriber doesn't want durable subscription, so no
      // don't bother with the acknowledgement callback
      cb = (doc) =>{
        callback(event, doc.message)
      }
    }
    this.on(event, cb)
    // todo: stop the listener when the last subscriber is removed
    return {
      unsubscribe: (done) => {
        this.removeListener(event, cb)
        // todo: when last listener is removed, close the cursor!
        done()
      }
    }
  }
  replay(event, opts, callback){
    let subscription = {
      subscribed: true,
      unsubscribe: function() {
        this.subscribed = false
        done()
      }
    }
    this.subscribersCollection.findOne({name: opts.name}, (err,info)=>{
      if (err) {
        this.emit('error',err)
      } else {
        if (subscription.subscribed) { // in case they already unsubscribed (weird!)
          if (info) {
            // existing subscriber, replay from last ack
            this.replayFrom(info.last, event, opts, callback, subscription)
          } else {
            // new subscriber, replay from the beginning
            this.replayFrom(null, event, opts, callback, subscription)
          }
        }
      }
    })
    return subscription
  }
  replayFrom(last, event, opts, callback, subscription){
    const query = last ? {
      _id: {
        $gt: last
      }
    } : {}
    const cursor = this.collection.find(query)
    subscription.unsubscribe = function(done){
      this.subscribed = false
      try {
        cursor.close()
      } catch (err) {
        this.emit('error',err)
      }
      done()
    }
    let prev = null
    const next = (err,doc) => {
      if (err) {
        this.emit('error',err)
        // todo: maybe need to try to recover - e.g. reconnect
      }
      if (subscription.subscribed) {
        // todo: maybe if collection is dropped or cursor closed for some external
        // reason we'll see no doc here but it isn't the end of the cursor?
        if (!doc) {
          subscription.unsubscribe = this.join(prev && prev._id, event, opts, callback).unsubscribe
          cursor.close()
        } else {
          const ev = (event === null ? 'message' : event)
          if ((ev === 'message') || (ev === doc.event)) {
            callback(doc.event, doc.message, (done)=>{
              this.ack(opts.name, doc._id, (err)=>{
                prev = doc
                // todo: retry the ack until it succeeds before
                // moving on to the next doc ?
                if (done) {
                  done(err)
                }
                if (subscription.subscribed) {
                  setImmediate(more)
                } else {
                  cursor.close()
                }
              })
            })
          } else {
            setImmediate(more)
          }
        }
      }
    }
    const more = () => {
      if (cursor && !cursor.isClosed())
        cursor.nextObject(next);
    }
    setImmediate(more)
    return subscription
  }
  ack(name,id,callback){
    this.subscribersCollection.updateOne({
      name: name
    }, {
      name: name,
      last: id
    }, {
     upsert: true,
     safe: true
   }, (err,result) => {
      if (callback) {
        if (err) {
          callback(err)
        } else {
          callback(null,result && result.upserted);
        }
      }
    })
  }
  latest(since, callback){
    const collection = this.collection
    if (since) {
      callback(null, {_id:since}, collection)
    } else {
      collection
        .find({})
        .sort({_id:-1})
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
  }
  ensureCollection(name,opts,callback) {
    this.connection.db.collections((err, collections)=>{
      const collection = collections.find(c => c.collectionName === name)
      if (collection) {
        callback(null, collection)
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
    return new Promise((resolve,reject)=>{
      if (!this.connection.db) {
        reject(new Error('not connected'))
      } else {
        this.ensureCollection(this.name, this.opts, (cerr,collection)=>{
          if (cerr) {
            reject(cerr)
          } else {
            this.collection = collection
            this.ensureCollection(`${this.name}_subscribers`, SUBSCRIBER_COLLECTION_OPTS, (serr,subscribersCollection)=>{
              if (serr){
                reject(serr)
              } else {
                this.subscribersCollection = subscribersCollection
                resolve(this)
              }
            })
          }
        })
      }
    })
  }
}

export default Topic
