import assert from 'assert'
import Connection from '../../lib/connection'

describe('connection', function() {

  describe('connect', function() {
    it('emits "error" event when connect fails', function(done) {
      new Connection('mongodb://localhost:666/doesnt_exist',{})
        .on('error', ()=>{
          done()
        })
        .connect()
    })
    it('emits "connect" event when connect succeeds', function(done) {
      new Connection('mongodb://localhost:27017/kafkaesque_test',{})
        .on('connect', ()=>{
          done()
        })
        .connect()
    })
  })

  describe('close', function(){
    it('invokes callback after closing connection', function(done){
      const conn = new Connection('mongodb://localhost:27017/kafkaesque',{})
      conn.on('connect',()=>{
        conn.close((err)=>{
          done()
        })
      })
      conn.connect()
    })
  })

})
