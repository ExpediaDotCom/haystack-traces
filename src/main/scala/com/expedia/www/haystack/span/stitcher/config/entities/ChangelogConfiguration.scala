package com.expedia.www.haystack.span.stitcher.config.entities

import java.util

/**
  * @param enabled enable the changelog for resiliency
  * @param logConfig configuration used for managing the changelog topics.
  *                  See <a href="https://github.com/apache/kafka/blob/0.10.2/core/src/main/scala/kafka/log/LogConfig.scala">LogConfig</a>
  */
case class ChangelogConfiguration(enabled: Boolean,
                                  logConfig: util.Map[String, String] = new util.HashMap[String, String]())
