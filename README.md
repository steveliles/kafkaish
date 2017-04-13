#Kafkaish

An experiment with an Apache-Kafka-like publish/subscribe mechanism based on MongoDB.

Q. Why not just use Kafka?

A. Actually I _really_ want to, and of course you should - if you have the resources. That said, in a resource constrained environment (little money, few people) where Mongo is already part of the infrastructure, sometimes the only choice you have is to use what you've already got. I need a moderately reliable Kafka-like mechanism without the additional resource requirements and overhead of deploying and managing kafka.

## Usage

Connect and create a topic:

```
const kafkaish = require('kafkaish')
    , connection = kafkaish('mongodb://localhost:27017/kafkaish')
connection.on('connect', function(){
  const topic = connection.topic('my_topic')
  topic.on('ready', function(){
    // use your topic
  })
})
connection.connect()

```

Publish messages to your topic fire-and-forget style:

```
topic.publish('event-name',{foo:'bar'})
```

Publish messages and receive a callback for confirmation of publishing:

```
topic.publish('event-name',{foo:bar},function(err){
  if (err) {
    // not published! try again or whatever
  } else {
    // yay, subscribers will receive the message
  }
})
```

Subscribe to receive specific events published from now on until you unsubscribe or otherwise disconnect:

```
topic.subscribe('event-name',{},function(ev,msg){
  // handle event
})
```

Use the returned subscription object to unsubscribe when you've had enough:

```
const count = 0
const subscription = topic.subscribe('event-name',{},function(ev,msg){
  count++
  if (count === 5) { // disconnect after 5 events
    subscription.unsubscribe()
  }
})
```

Subscribe to receive ALL events published from now on until you unsubscribe or otherwise disconnect:

```
topic.subscribe(null,{},function(ev,msg){
  // handle event
})
```

Subscribe a durable subscription receive specific events. You can unsubscribe/disconnect and come back later to collect events that occurred while you were away:

```
topic.subscribe(null,{name:'durable-subscriber-1'},function(ev,msg,ack){
  // ... do some cool stuff
  // ...
  ack() // acknowledge this message so we don't replay it if we re-connect
})
```

Subscribe a durable subscription receive specific events, and replay the backlog of events that already occurred. You can unsubscribe/disconnect and come back later to collect events that occurred while you were away:

```
topic.subscribe(null,{name:'durable-subscriber-1',replay:true},function(ev,msg,ack){
  // ... do some cool stuff
  // ...
  ack() // acknowledge this message so we don't replay it if we re-connect
})
```
