import assert from 'assert'
import Topic from '../../lib/topic'
import Connection from '../../lib/Connection'

const URI = 'mongodb://localhost:27017/kafkaish_test'

describe('topic', function() {

  let connection = null

  before(function(){
    connection = new Connection(URI,{})
  })

  after(function(done){
    if (connection && connection.db) {
      connection.db.dropDatabase(done)
      connection = null
    }
  })

  describe('create', function() {
    it('emits "not-connected" if not connected', function(done) {
      // we didn't call connect ...
      const topic = connection.topic('test')
      topic.on('not-connected', done)
    })
    it('emits "ready" when topic infrastructure is prepared', function(done) {
      connection
        .on('connect', ()=>{
          const topic = connection.topic('test')
          topic.on('ready', done)
          topic.create()
        })
        .connect()
    })
  })

})
