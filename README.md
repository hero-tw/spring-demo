# Placeholder spring boot jar

Prerequisites: 

Cassandra stub

Run the embedded Cassandra server from [cassandra-stub](https://github.com/hero-tw/cassandra-stub)

Kinesis Stream

* Run the [Terraform configuration for Kinesis](https://github.com/hero-tw/kinesis-infrastructure)
* Set the environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY on Intellij's Run Configurations.

Usage:

- gradle bootRun
