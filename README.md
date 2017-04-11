An experiment with an Apache-Kafka-like publish/subscribe mechanism based on MongoDB.

Q. Why not just use Kafka?
A. Well of course you should if you have the resources. I happen to be working in a resource constrained environment (little money, few people) where Mongo is already part of the infrastructure. I need a moderately reliable Kafka-like mechanism without the additional resource requirements and overhead of deploying and managing kafka.
