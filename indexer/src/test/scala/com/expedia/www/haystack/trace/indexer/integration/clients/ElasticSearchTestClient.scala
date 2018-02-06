/*
 *  Copyright 2017 Expedia, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.expedia.www.haystack.trace.indexer.integration.clients

import java.text.SimpleDateFormat
import java.util.Date

import com.expedia.www.haystack.trace.commons.config.entities.{WhiteListIndexFields, WhitelistIndexField, WhitelistIndexFieldConfiguration}
import com.expedia.www.haystack.trace.commons.retries.RetryOperation
import com.expedia.www.haystack.trace.indexer.config.entities.ElasticSearchConfiguration
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Search
import io.searchbox.indices.DeleteIndex
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

case class EsSourceDocument(traceid: String)

class ElasticSearchTestClient {
  implicit val formats = DefaultFormats

  private val ELASTIC_SEARCH_ENDPOINT = "http://elasticsearch:9200"
  private val INDEX_NAME_PREFIX = "haystack-traces"
  private val INDEX_TYPE = "spans"
  private val INDEX_HOUR_BUCKET = 6

  private val HAYSTACK_TRACES_INDEX = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    s"$INDEX_NAME_PREFIX-${formatter.format(new Date())}"
  }

  private val esClient: JestClient = {
    val factory = new JestClientFactory()
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ELASTIC_SEARCH_ENDPOINT).build())
    factory.getObject
  }

  def prepare(): Unit = {
    // drop the haystack-traces-<today's date> index
    0 until (24 / INDEX_HOUR_BUCKET) foreach {
      idx => {
        esClient.execute(new DeleteIndex.Builder(s"$HAYSTACK_TRACES_INDEX-$idx").build())
      }
    }
  }

  def buildConfig = ElasticSearchConfiguration(
    ELASTIC_SEARCH_ENDPOINT,
    Some(INDEX_TEMPLATE),
    "one",
    INDEX_NAME_PREFIX,
    INDEX_HOUR_BUCKET,
    INDEX_TYPE,
    3000,
    3000,
    10,
    10,
    10,
    RetryOperation.Config(3, 2000, 2))

  def indexingConfig: WhitelistIndexFieldConfiguration = {
    val cfg = WhitelistIndexFieldConfiguration()
    val cfgJsonData = Serialization.write(WhiteListIndexFields(
      List(WhitelistIndexField(name = "role", `type` = "string"), WhitelistIndexField(name = "errorcode", `type` = "long"))))
    cfg.onReload(cfgJsonData)
    cfg
  }

  def query(query: String): List[EsSourceDocument] = {
    import scala.collection.JavaConversions._
    val searchQuery = new Search.Builder(query)
      .addIndex(INDEX_NAME_PREFIX)
      .addType(INDEX_TYPE)
      .build()
    val result = esClient.execute(searchQuery)
    if(result.getSourceAsStringList != null && result.getSourceAsStringList.size() > 0) {
      result.getSourceAsStringList.map(Serialization.read[EsSourceDocument]).toList
    }
    else {
      Nil
    }
  }

  private val INDEX_TEMPLATE =
    """{
      |    "template": "haystack-traces*",
      |    "settings": {
      |        "number_of_shards": 1,
      |        "index.mapping.ignore_malformed": true,
      |        "analysis": {
      |            "normalizer": {
      |                "lowercase_normalizer": {
      |                    "type": "custom",
      |                    "filter": ["lowercase"]
      |                }
      |            }
      |        }
      |    },
      |    "aliases": {
      |        "haystack-traces": {}
      |    },
      |    "mappings": {
      |        "spans": {
      |            "_field_names": {
      |                "enabled": false
      |            },
      |            "_all": {
      |                "enabled": false
      |            },
      |            "_source": {
      |                "includes": ["traceid"]
      |            },
      |            "properties": {
      |                "spans": {
      |                    "type": "nested",
      |                    "properties": {
      |                        "servicename": {
      |                            "type": "keyword",
      |                            "normalizer": "lowercase_normalizer",
      |                            "doc_values": true,
      |                            "norms": false
      |                        },
      |                        "operationname": {
      |                            "type": "keyword",
      |                            "normalizer": "lowercase_normalizer",
      |                            "doc_values": true,
      |                            "norms": false
      |                        }
      |                    }
      |                }
      |            },
      |            "dynamic_templates": [{
      |                "strings_as_keywords_1": {
      |                    "match_mapping_type": "string",
      |                    "mapping": {
      |                        "type": "keyword",
      |                        "normalizer": "lowercase_normalizer",
      |                        "doc_values": false,
      |                        "norms": false
      |                    }
      |                }
      |            }, {
      |                "longs_disable_doc_norms": {
      |                    "match_mapping_type": "long",
      |                    "mapping": {
      |                        "type": "long",
      |                        "doc_values": false,
      |                        "norms": false
      |                    }
      |                }
      |            }]
      |        }
      |    }
      |}
      |""".stripMargin
}
