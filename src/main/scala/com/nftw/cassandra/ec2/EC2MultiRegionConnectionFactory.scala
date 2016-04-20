package com.nftw.cassandra.ec2

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core.{Cluster, JdkSSLOptions, SSLOptions, SocketOptions}
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf, LocalNodeFirstLoadBalancingPolicy, MultipleRetryPolicy}
import org.apache.commons.io.IOUtils

/** Based on com.datastax.spark.connector.cql.DefaultConnectionFactory */
object EC2MultiRegionConnectionFactory extends CassandraConnectionFactory  {

    /** Returns the Cluster.Builder object used to setup Cluster instance. */
    def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
      val options = new SocketOptions()
        .setConnectTimeoutMillis(conf.connectTimeoutMillis)
        .setReadTimeoutMillis(conf.readTimeoutMillis)

      val builder = Cluster.builder()
        .addContactPoints(conf.hosts.toSeq: _*)
        .withPort(conf.port)
        .withRetryPolicy(
          new MultipleRetryPolicy(conf.queryRetryCount, conf.queryRetryDelay))
        .withReconnectionPolicy(
          new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
        .withLoadBalancingPolicy(
          new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
        .withAuthProvider(conf.authConf.authProvider)
        .withSocketOptions(options)
        .withCompression(conf.compression)
        // The magic happens here :)
        .withAddressTranslator(EC2MultiRegionAddressTranslatorFactory.create)

      if (conf.cassandraSSLConf.enabled) {
        maybeCreateSSLOptions(conf.cassandraSSLConf) match {
          case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
          case None ⇒ builder.withSSL()
        }
      } else {
        builder
      }
    }

    private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
      conf.trustStorePath map {
        case path ⇒

          val trustStoreFile = new FileInputStream(path)
          val tmf = try {
            val keyStore = KeyStore.getInstance(conf.trustStoreType)
            conf.trustStorePassword match {
              case None ⇒ keyStore.load(trustStoreFile, null)
              case Some(password) ⇒ keyStore.load(trustStoreFile, password.toCharArray)
            }
            val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
            tmf.init(keyStore)
            tmf
          } finally {
            IOUtils.closeQuietly(trustStoreFile)
          }

          val context = SSLContext.getInstance(conf.protocol)
          context.init(null, tmf.getTrustManagers, new SecureRandom)
          JdkSSLOptions.builder()
            .withSSLContext(context)
            .withCipherSuites(conf.enabledAlgorithms.toArray)
            .build()
      }
    }

    /** Creates and configures native Cassandra connection */
    override def createCluster(conf: CassandraConnectorConf): Cluster = {
      clusterBuilder(conf).build()
    }
  }
