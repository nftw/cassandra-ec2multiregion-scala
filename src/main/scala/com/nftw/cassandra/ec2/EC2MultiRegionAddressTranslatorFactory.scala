package com.nftw.cassandra.ec2

import java.lang.reflect.{InvocationTargetException, Constructor}
import javax.naming.{NamingException, Context}
import javax.naming.directory.InitialDirContext

import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator

object EC2MultiRegionAddressTranslatorFactory {
  /** Creates a com.datastax.driver.core.policies.EC2MultiRegionAddressTranslator with the naming provider overridden to
    * the Amazon DNS server IP address 169.254.169.253.
    *
    * See http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_DHCP_Options.html
    */
  def create: EC2MultiRegionAddressTranslator = {
    // Create
    val env: java.util.Hashtable[AnyRef, AnyRef] = new java.util.Hashtable[AnyRef, AnyRef]
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
    env.put("java.naming.provider.url", "dns://169.254.169.253")
    var ctx: InitialDirContext = null
    try {
      ctx = new InitialDirContext(env)
    }
    catch {
      case e: NamingException => throw new DriverException("Could not create translator", e)
    }

    // Hack to inject the InitialDirContext into the EC2MultiRegionAddressTranslator.
    val constructors: Array[Constructor[_]] = classOf[EC2MultiRegionAddressTranslator].getDeclaredConstructors
    for (constructor <- constructors) {
      if (constructor.getParameterTypes.length == 1) {
        constructor.setAccessible(true)
        try {
          return constructor.newInstance(ctx).asInstanceOf[EC2MultiRegionAddressTranslator]
        }
        catch {
          case e: InstantiationException => throw new DriverException("Could not create translator", e)
          case e: IllegalAccessException => throw new DriverException("Could not create translator", e)
          case e: InvocationTargetException => throw new DriverException("Could not create translator", e)
        }
      }
    }
    throw new DriverException("Could not create translator - Failed to locate required EC2MultiRegionAddressTranslator constructor")
  }
}
