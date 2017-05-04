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
      return new Promise((resolve,reject)=>{
        resolve(this)
      })
    } else {
      return new Promise((resolve,reject)=>{
        MongoClient.connect(this.uri, this.opts, (err, db) => {
          if (err) {
            reject(err)
          } else {
            this.db = db;
            db.on('error', (err) => {
              this.emit('error', err);
            });
            resolve(this)
          }
        });
      })
    }
    return this
  }
  prepareTopic(name, opts={}){
    return new Promise((resolve,reject)=>{
      if (!this.topics[name] || this.topics[name].closed) {
        const t = new Topic(this, name, opts);
        t.create()
          .then(topic => {
            this.topics[name] = topic
            resolve(topic)
          })
          .catch(err => reject(err))
      } else {
        resolve(this.topics[name]);
      }
    })

  }
  close(errBack){
    this.db.close(errBack)
  }
}

export default Connection
