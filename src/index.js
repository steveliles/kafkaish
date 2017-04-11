import Connection from './connection'

const Kafkaesque = function(uri,opts={}){
  return new Connection(uri,opts).connect()
}

export default Kafkaesque
