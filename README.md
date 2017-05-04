# Kafkaish

Stability: _very unstable / experimental._

Durable message queues are handy when you only have a few consumers, but they're a bit unwieldy and wasteful when you have many consumers or consumers come and go over time.

Message topics are handy when you have large/varying numbers of subscribers that come and go, but sometimes you need each consumer to receive all messages regardless whether they were connected when the message was dispatched.

Kafka topics have a nice mix of the properties of topics and queues - publish/subscribe plus guaranteed delivery.

This lib is an experiment with an Apache-Kafka-like publish/subscribe mechanism based on MongoDB.

Q. Why not just use Kafka?

A. Actually I _really_ want to, and of course you should - if you have the resources. That said, in a resource constrained environment (little money, few people) where Mongo is already part of the infrastructure, sometimes the only choice you have is to use what you've already got. I need a moderately reliable Kafka-like mechanism without the additional resource requirements and overhead of deploying and managing kafka.

## Usage

Bring kafkaish into scope:

```javascript
const kafkaish = require('kafkaish')
```

Connect and create a topic:

```javascript
kafkaish('mongodb://localhost:27017/kafkaish').connect()
  .then(conn => {
    conn.prepareTopic('my_topic')
      .then(topic => {
        // use your topic
      })
  })
```

Publish messages to your topic fire-and-forget style:

```javascript
topic.publish('event-name',{foo:'bar'})
```

Publish messages and receive a callback for confirmation of publishing:

```javascript
topic.publish('event-name',{foo:bar},function(err){
  if (err) {
    // not published! try again or whatever
  } else {
    // yay, subscribers will receive the message
  }
})
```

Subscribe to receive specific events published from now on until you unsubscribe or otherwise disconnect:

```javascript
topic.subscribe('event-name',{},function(ev,msg){
  // handle event
})
```

Use the returned subscription object to unsubscribe when you've had enough:

```javascript
const count = 0
const subscription = topic.subscribe('event-name',{},function(ev,msg){
  count++
  if (count === 5) { // disconnect after 5 events
    subscription.unsubscribe()
  }
})
```

Subscribe to receive ALL events published from now on until you unsubscribe or otherwise disconnect:

```javascript
topic.subscribe(null,{},function(ev,msg){
  // handle event
})
```

Subscribe a durable subscription to receive specific events. You can unsubscribe/disconnect and come back later to collect events that occurred while you were away:

```javascript
topic.subscribe('some-event',{name:'durable-subscriber-1'},function(ev,msg,ack){
  // here we'll see any events published from now on.
  // if we disconnect and re-connect with "replay:true"
  ack() // acknowledge this message so we don't replay it if we re-connect
})
```

Subscribe a durable subscription to receive specific events, replaying the backlog of events that already occurred. You can unsubscribe/disconnect and come back later to collect events that occurred while you were away:

```javascript
topic.subscribe('some-event',{name:'durable-subscriber-1',replay:true},function(ev,msg,ack){
  // here we'll see all events that already occurred before settling in to receive
  // events in real-time as they are published. We need to ack() each event.
  ack()
})
```
