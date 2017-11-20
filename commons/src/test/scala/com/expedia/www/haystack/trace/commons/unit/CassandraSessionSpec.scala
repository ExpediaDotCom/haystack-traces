package com.expedia.www.haystack.trace.commons.unit

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.Insert
import com.expedia.www.haystack.trace.commons.clients.cassandra.{CassandraClusterFactory, CassandraSession}
import com.expedia.www.haystack.trace.commons.config.entities.{CassandraConfiguration, SocketConfiguration}
import org.easymock.EasyMock
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FunSpec, Matchers}

class CassandraSessionSpec extends FunSpec with Matchers with EasyMockSugar {
  describe("Cassandra Session") {
    it("should connect to the cassandra cluster and provide prepared statement for inserts") {
      val keyspaceName = "keyspace-1"
      val tableName = "table-1"

      val factory = mock[CassandraClusterFactory]
      val session = mock[Session]
      val cluster = mock[Cluster]
      val metadata = mock[Metadata]
      val keyspaceMetadata = mock[KeyspaceMetadata]
      val tableMetadata = mock[TableMetadata]
      val insertPrepStatement = mock[PreparedStatement]

      val config = CassandraConfiguration(List("cassandra1"),
        autoDiscoverEnabled = false,
        None,
        None,
        keyspaceName,
        tableName,
        None,
        SocketConfiguration(10, keepAlive = true, 1000, 1000))

      val captured = EasyMock.newCapture[Insert.Options]()
      expecting {
        factory.buildCluster(config).andReturn(cluster).once()
        cluster.connect().andReturn(session).once()
        keyspaceMetadata.getTable(tableName).andReturn(tableMetadata).once()
        metadata.getKeyspace(keyspaceName).andReturn(keyspaceMetadata).once()
        cluster.getMetadata.andReturn(metadata).once()
        session.getCluster.andReturn(cluster).once()
        session.execute("USE " + config.keyspace).andReturn(null).once
        session.prepare(EasyMock.capture(captured)).andReturn(insertPrepStatement).anyTimes()
        session.close().once()
        cluster.close().once()
      }

      whenExecuting(factory, cluster, session, metadata, keyspaceMetadata, tableMetadata, insertPrepStatement) {
        val session = new CassandraSession(config, factory)
        val stmt = session.createInsertPreparedStatement(100)
        stmt shouldBe insertPrepStatement
        captured.getValue.getQueryString() shouldEqual "INSERT INTO \"table-1\" (id,ts,spans) VALUES (:id,:ts,:spans) USING TTL 100;"
        session.close()
      }
    }
  }
}
