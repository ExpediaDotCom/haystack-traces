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
    esClient.execute(new DeleteIndex.Builder(HAYSTACK_TRACES_INDEX).build())
  }

  def buildConfig = ElasticSearchConfiguration(
    ELASTIC_SEARCH_ENDPOINT,
    Some(INDEX_TEMPLATE),
    "one",
    INDEX_NAME_PREFIX,
    INDEX_TYPE,
    3000,
    3000,
    10,
    10,
    10)

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
      .addIndex(HAYSTACK_TRACES_INDEX)
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

  private val INDEX_TEMPLATE = """
                                 |{
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
                                 |            "_all": { "enabled": false },
                                 |            "_source": {
                                 |               "includes": ["traceid"]
                                 |            },
                                 |            "properties": {
                                 |                "spans": {
                                 |                    "type": "nested"
                                 |                },
                                 |                "traceid": {
                                 |                    "enabled": false
                                 |                }
                                 |            },
                                 |            "dynamic_templates": [
                                 |                {
                                 |                    "strings_as_keywords_1": {
                                 |                        "match_mapping_type": "string",
                                 |                        "match_pattern": "regex",
                                 |                        "unmatch": "^(service|operation)$",
                                 |                        "mapping": {
                                 |                            "type": "keyword",
                                 |                            "normalizer": "lowercase_normalizer",
                                 |                            "doc_values": false
                                 |                        }
                                 |                    }
                                 |                },
                                 |                {
                                 |                    "strings_as_keywords_2": {
                                 |                        "match_mapping_type": "string",
                                 |                        "match_pattern": "regex",
                                 |                        "match": "^(service|operation)$",
                                 |                        "mapping": {
                                 |                            "type": "keyword",
                                 |                            "normalizer": "lowercase_normalizer",
                                 |                            "doc_values": true
                                 |                        }
                                 |                    }
                                 |                }
                                 |            ]
                                 |        }
                                 |    }
                                 |}
                               """.stripMargin

}
