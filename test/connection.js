import assert from 'assert'
import Connection from '../lib/connection'

describe('connection', function() {

  describe('connect', function() {
    it('rejects promise with error when connect fails', function(done) {
      new Connection('mongodb://localhost:666/doesnt_exist',{}).connect()
        .then(()=>{done(new Error('expected an error!'))})
        .catch(()=>{done()}) // expected
    })
    it('resolves promise to the Connection object when connect succeeds', function(done) {
      const conn = new Connection('mongodb://localhost:27017/kafkaesque_test',{})
      conn.connect()
        .then((c)=>{
          assert(c === conn)
          done()
        })
        .catch(done)
    })
  })

  describe('close', function(){
    it('invokes callback after closing connection', function(done){
      new Connection('mongodb://localhost:27017/kafkaesque',{}).connect()
        .then(c => c.close((err=>{done()})))
        .catch(done)
    })
  })

})
