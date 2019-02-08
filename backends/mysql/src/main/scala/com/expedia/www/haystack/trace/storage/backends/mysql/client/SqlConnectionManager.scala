package com.expedia.www.haystack.trace.storage.backends.mysql.client

import java.sql.Connection

import com.expedia.www.haystack.trace.storage.backends.mysql.config.entities.ClientConfiguration
import org.apache.commons.dbcp.BasicDataSource
import org.slf4j.{Logger, LoggerFactory}

class SqlConnectionManager(config: ClientConfiguration) extends AutoCloseable {

  private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)

  private lazy val connectionPool: BasicDataSource = {

    val basicDataSource = new BasicDataSource
    basicDataSource.setDriverClassName(config.driver)
    basicDataSource.setUrl(config.endpoints)
    basicDataSource.setMaxActive(config.socket.maxConnectionPerHost)
    config.plaintextCredentials.foreach { credentials =>
      basicDataSource.setPassword(credentials.password)
      basicDataSource.setUsername(credentials.username)
    }
    basicDataSource
  }

  def getConnection: Connection = {
    connectionPool.getConnection
  }

  override def close(): Unit = {
    connectionPool.close()
  }


}
