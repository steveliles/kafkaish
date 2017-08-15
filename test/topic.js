import assert from 'assert'
import Topic from '../lib/topic'
import Connection from '../lib/Connection'

const URI = 'mongodb://localhost:27017/kafkaish_test'

describe('topic', function() {

  let connection = null
  let d = null

  beforeEach(function(){
    d = require('domain').create();
    d.on('error',(err)=>{
      if (err.name === 'MongoError'){
        // ignore
        console.error(err)
      } else {
        console.error(err)
      }
    })
    connection = new Connection(URI,{})
    connection.on('error',console.error)
  })

  afterEach(function(done){
    // we're fairly drastically shutting down operations by
    // dropping the collection while cursors are potentially
    // still open. Need to find a cleaner way around this than
    // hacking it with domains and silently ignored errors :S
    setTimeout(()=>{
      d.run(()=>{
        if (connection && connection.db) {
          try {
            connection.db.dropDatabase(()=>{
              connection.db.close(()=>{
                setTimeout(done, 100)
              })
              connection = null
            })
          } catch (err) {
            console.log(err)
            setTimeout(done, 100)
          }
        } else {
          setTimeout(done, 100)
        }
      })
    }, 50)
  })

  describe('create', function() {
    it('fails with error if not connected', function(done) {
      // we didn't call connect ...
      connection.prepareTopic('test-create')
        .then(topic => done(new Error('should not have worked, we arent connected!')))
        .catch(err => {
          if (err.message === 'not connected') {
            done()
          } else {
            done(err)
          }
        }) // expected error
    })
    it('resolves promise when topic infrastructure is prepared', function(done) {
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-create')
            .then(topic => done())
            .catch(done)
        })
        .catch(done)
    })
  })

  describe('publish', function(){
    it('publishes message and calls back on success/failure', function(done){
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-publish')
            .then(topic => {
              topic.publish('some-event',{},(err,result)=>{
                topic.close()
                done()
              })
            })
        })
    })
  })

  describe('subscribe', function(){

    it('allows non-durable subscription by omitting name from opts', function(done){
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-subscribe-1')
            .then(topic => {
              topic.publish('my-event',{number:1},()=>{
                const subscription = topic.subscribe('my-event',{},(event,message)=>{
                  // non-durable, should not see the previous message
                  if (message.number === 2) {
                    subscription.unsubscribe(()=>{
                      topic.close()
                      done()
                    })
                  } else {
                    topic.close()
                    done(new Error("should have received event number 2"))
                  }
                })
                topic.publish('my-event',{number:2})
              })
            })
            .catch(done)
        })
    })
    it ('allows durable subscription and replay to named subscribers in opts', function(done){
      let didReceiveMessage1 = false
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-subscribe-2')
            .then(topic => {
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
                      topic.close()
                      done()
                    } else {
                      topic.close()
                      done(new Error("should have received message 1 first!"))
                    }
                  }
                })
                topic.publish('my-event',{number:2})
              })
            })
            .catch(done)
        })
    })
    it ('doesnt receive more events after unsubscribe', function(done){
      let received = false
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-subscribe-3')
            .then(topic => {
              topic.publish('my-event',{number:1},()=>{
                const subscription = topic.subscribe('my-event',{
                  name:'a-durable-subscriber',
                  replay: true
                },(event,message,ack)=>{
                  if (received) {
                    topic.close()
                    done(new Error('only expected to receive one message!'))
                  } else {
                    received = true
                    subscription.unsubscribe(()=>{
                      topic.publish('my-event',{number:2},()=>{
                        setTimeout(()=>{
                          topic.close()
                          done()
                        },500)
                      })
                    })
                    ack()
                  }
                })
              })
            })
            .catch(done)
        })
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
          if (count > expect) {
            topic.close()
            done(new Error(`${first}-${last} saw ${count} events but expected only ${expect}`))
          } else {
            ack()
            if (count === 1) {
              if (message.number !== first){
                topic.close()
                done(new Error(`expected ${first} but saw ${message.number}`))
              }
            }
            if (count === expect) {
              if (message.number !== last){
                topic.close()
                done(new Error(`expected ${last} but saw ${message.number}`))
              }
              subscription.unsubscribe(()=>{
                if (then)
                  then()
              })
            }
          }
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
      this.timeout(65000);
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-subscribe-4')
            .then(topic => {
              publish(topic,1,6,()=>{
                receive(topic,1,5,()=>{
                  publish(topic,7,8,()=>{
                    receive(topic,6,10,()=>{
                      topic.close()
                      done()
                    })
                    publish(topic,9,10)
                  })
                })
              })
            })
        })
        .catch(done)
    })
    it ('if message not acknowledged, subscriber will receive again at next subscribe', function(done){
      const receive = (topic,first,last,then) => {
        let count = 0
          , expect = (last-first)+1
        const subscription = topic.subscribe('my-event',{
          name:'a-durable-subscriber',
          replay: true
        },(event,message,ack)=>{
          count++
          if (count > expect) {
            if (count > expect+1) {
              topic.close()
              done(new Error(`${first}-${last} saw ${count} events but expected only ${expect}`))
            } else {
              // unsubscribe without ack'ing this last message
              subscription.unsubscribe(()=>{
                if (then)
                  then()
              })
            }
          } else {
            if (count === 1) {
              if (message.number !== first){
                done(new Error(`expected ${first} but saw ${message.number}`))
              }
            }
            if (count === expect) {
              if (message.number !== last){
                topic.close()
                done(new Error(`expected ${last} but saw ${message.number}`))
              }
            }
            ack()
          }
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
      connection.connect()
        .then(conn => {
          conn.prepareTopic('test-subscribe-5')
            .then(topic => {
              publish(topic,1,6,()=>{
                receive(topic,1,4,()=>{
                  publish(topic,7,8,()=>{
                    receive(topic,5,10,()=>{
                      topic.close()
                      done()
                    })
                    setTimeout(()=>{
                      publish(topic,9,11)
                    },500)
                  })
                })
              })
            })
        })
        .catch(done)
    })
  })
  it ('up-to-date subscriber correctly acknowledges new events', function(done){
    const receive = (topic,first,last,then) => {
      let count = 0
        , expect = (last-first)+1
      const subscription = topic.subscribe('my-event',{
        name:'a-durable-subscriber',
        replay: true
      },(event,message,ack)=>{
        count++
        if (count > expect) {
          topic.close()
          done(new Error(`${first}-${last} saw ${count} events but expected only ${expect}`))
        } else {
          if (count === 1) {
            if (message.number !== first){
              topic.close()
              done(new Error(`expected ${first} but saw ${message.number}`))
            }
          } else {
            if (count === expect) {
              subscription.unsubscribe(()=>{
                ack(()=>{
                  if (then)
                    then()
                })
              })
              if (message.number !== last){
                topic.close()
                done(new Error(`expected ${last} but saw ${message.number}`))
              }
            }
          }
        }
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
    connection.connect()
      .then(conn => {
        conn.prepareTopic('test-subscribe-6')
          .then(topic => {
            receive(topic,1,4,()=>{
              receive(topic,5,10,()=>{
                topic.close()
                done()
              })
              setTimeout(()=>{
                publish(topic,5,10,()=>{})
              },1000)
            })
            // give the receiver a chance to be ready
            // before pushing the first events ...
            setTimeout(()=>{
              publish(topic,1,4,()=>{})
            },250)
          })
      })
      .catch(done)
  })

})
