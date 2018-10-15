# Placeholder spring boot jar

Prerequisites: 

Cassandra stub

Run the embedded Cassandra server from [cassandra-stub](https://github.com/hero-tw/cassandra-stub)

Kinesis Stream

* Run the [Terraform configuration for Kinesis](https://github.com/hero-tw/kinesis-infrastructure)
* Set the environment variables AWS_ACCESS_KEY and AWS_SECRET_KEY on Intellij's Run Configurations.
* The http body for the kinesis POST endpoint must look like this:

```$xslt
{
	"id": 123,
	"name": "A User",
	"company": "A Company"
}
```

Usage:

- gradle bootRun
