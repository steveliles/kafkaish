import Connection from './connection'

const Kafkaish = function(uri,opts={}){
  return new Connection(uri,opts)
}

export default Kafkaish
