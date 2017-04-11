import { EventEmitter } from 'events'
import Topic from './topic'

const MongoClient = require('mongodb').MongoClient

class Connection extends EventEmitter {
  constructor(uri, opts){
    super()
    this.uri = uri
    this.opts = opts
    this.topics = {};
  }
  connect(){
    this.opts.autoReconnect != null || (this.opts.autoReconnect = true)
    if (this.uri.collection) {
      // uri is already a db instance
      this.db = this.uri;
    } else {
      MongoClient.connect(this.uri, this.opts, (err, db) => {
        if (err) {
          this.emit('error', err)
        } else {
          this.db = db;
          this.emit('connect', this);
          db.on('error', (err) => {
            this.emit('error', err);
          });
        }
      });
    }
    return this
  }
  topic(name, opts={}){
    if (!this.topics[name] || this.topics[name].closed) {
      this.topics[name] = new Topic(this, name, opts);
    }
    return this.topics[name];
  }
  close(errBack){
    this.db.close(errBack)
  }
}

export default Connection
