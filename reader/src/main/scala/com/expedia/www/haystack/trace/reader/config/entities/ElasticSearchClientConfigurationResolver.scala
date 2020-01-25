package com.expedia.www.haystack.trace.reader.config.entities

trait ElasticSearchClientConfigurationResolver {
  def get: Seq[ElasticSearchClientConfiguration]
}
