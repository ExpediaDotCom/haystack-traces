package com.expedia.www.haystack.trace.reader.config.entities

case class StaticElasticSearchClientConfigurationResolver(elasticSearchClientConfiguration: Seq[ElasticSearchClientConfiguration]) extends ElasticSearchClientConfigurationResolver {
  override def get: Seq[ElasticSearchClientConfiguration] = elasticSearchClientConfiguration
}