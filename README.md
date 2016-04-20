# cassandra-ec2multiregion-scala
Allows use of Datastax's EC2MultiRegionAddressTranslator in Scala with the DNS server set to the VPC default.


## Usage

### DataStax Spark Cassandra Connector:

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", myCassandraHost)
      .set("spark.cassandra.connection.factory", "EC2MultiRegionConnectionFactory")
    val sc = new SparkContext(conf)

### Phantom DSL:

    val addressTranslator =    EC2MultiRegionAddressTranslatorFactory.create
    val keySpace = ContactPoints(Seq("my_host"))
      .withClusterBuilder((builder) => {
        builder.withAddressTranslator(addressTranslator)
       })
      .keySpace("my_keyspace")