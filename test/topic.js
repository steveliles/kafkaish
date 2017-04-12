import assert from 'assert'
import Topic from '../lib/topic'
import Connection from '../lib/Connection'

const URI = 'mongodb://localhost:27017/kafkaish_test'

describe('topic', function() {

  let connection = null

  beforeEach(function(){
    connection = new Connection(URI,{})
    connection.on('error',console.error)
  })

  afterEach(function(done){
    // we're fairly drastically shutting down operations by
    // dropping the collection while cursors are potentially
    // still open. Need to find a cleaner way around this than
    // hacking it with domains and silently ignored errors :S
    const d = require('domain').create();
    d.on('error',(err)=>{
      if (err.name === 'MongoError'){
        // ignore
      } else {
        console.error(err)
      }
    })
    d.run(()=>{
      if (connection && connection.db) {
        try {
          connection.db.dropDatabase(()=>{
            connection.db.close(()=>{
              done()
            })
            connection = null
          })
        } catch (err) {
          console.log(err)
          done()
        }
      } else {
        done()
      }
    })
  })

  describe('create', function() {
    it('emits "not-connected" if not connected', function(done) {
      // we didn't call connect ...
      const topic = connection.topic('test-create')
      topic.on('not-connected', done)
    })
    it('emits "ready" when topic infrastructure is prepared', function(done) {
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-create')
          topic.on('ready', ()=>{
            done()
          })
        })
        .connect()
    })
  })

  describe('publish', function(){
    it('publishes message and calls back with success/failure', function(done){
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-publish')
          topic.on('ready', ()=>{
            topic.publish('some-event',{},()=>{
              done()
            })
          })
        })
        .connect()
    })
  })

  describe('subscribe', function(){
    it('allows non-durable subscription by omitting name from opts', function(done){
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-subscribe')
          topic.on('ready', ()=>{
            topic.publish('my-event',{number:1},()=>{
              const subscription = topic.subscribe('my-event',{},(event,message)=>{
                // non-durable, should not see the previous message
                if (message.number === 2) {
                  subscription.unsubscribe(done)
                } else {
                  done(new Error("should have received event number 2"))
                }
              })
              topic.publish('my-event',{number:2})
            })
          })
        })
        .connect()
    })
    it ('allows durable subscription and replay to named subscribers in opts', function(done){
      let didReceiveMessage1 = false
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-subscribe')
          topic.on('ready', ()=>{
            topic.publish('my-event',{number:1},()=>{
              topic.subscribe('my-event',{
                name:'a-durable-subscriber',
                replay: true
              },(event,message,ack)=>{
                if (message.number === 1) {
                  didReceiveMessage1 = true
                  ack()
                }
                if (message.number === 2) {
                  if (didReceiveMessage1) {
                    done()
                  } else {
                    done(new Error("should have received message 1 first!"))
                  }
                }
              })
              topic.publish('my-event',{number:2})
            })
          })
        })
        .connect()
    })
    it ('doesnt receive more events after unsubscribe', function(done){
      let received = false
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-subscribe')
          topic.on('ready', ()=>{
            topic.publish('my-event',{number:1},()=>{
              const subscription = topic.subscribe('my-event',{
                name:'a-durable-subscriber',
                replay: true
              },(event,message,ack)=>{
                if (received) {
                  done(new Error('only expected to receive one message!'))
                } else {
                  received = true
                  ack()
                  subscription.unsubscribe(()=>{
                    topic.publish('my-event',{number:2},()=>{
                      setTimeout(done,500)
                    })
                  })
                }
              })
            })
          })
        })
        .connect()
    })
    it ('durable subscribers can replay from last acknowledged position', function(done){
      const receive = (topic,first,last,then) => {
        let count = 0
          , expect = (last-first)+1
        const subscription = topic.subscribe('my-event',{
          name:'a-durable-subscriber',
          replay: true
        },(event,message,ack)=>{
          count++
          if (count > expect)
            done(new Error(`${first}-${last} saw ${count} events but expected only ${expect}`))
          if (count === 1) {
            if (message.number !== first){
              done(new Error(`expected ${first} but saw ${message.number}`))
            }
          }
          if (count === expect) {
            if (message.number !== last){
              done(new Error(`expected ${last} but saw ${message.number}`))
            }
            subscription.unsubscribe(()=>{
              if (then)
                then()
            })
          }
          ack()
        })
      }
      const publish = (topic,from,to,then) => {
        topic.publish('my-event',{number:from},(err,doc)=>{
          if (from === to) {
            if (then)
              then()
          } else {
            publish(topic,from+1,to,then)
          }
        })
      }
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test-subscribe')
          topic.on('ready', ()=>{
            publish(topic,1,6,()=>{
              receive(topic,1,5,()=>{
                publish(topic,7,8,()=>{
                  receive(topic,6,10,done)
                  publish(topic,9,10)
                })
              })
            })
          })
        })
        .connect()
    })
  })

})
