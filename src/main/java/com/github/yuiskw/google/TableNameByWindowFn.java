/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.yuiskw.google;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

/**
 * Change table which is partitioned by date based on the window
 */
public class TableNameByWindowFn implements SerializableFunction<BoundedWindow, String> {

  String projectId = null;
  String datasetId = null;
  String tablePrefix = null;
  DateTimeZone timeZone = DateTimeZone.UTC;

  /**
   * Make an instance
   *
   * @param projectId Google Cloud project ID
   * @param datasetId BigQuery dataset ID
   * @param tablePrefix BigQuery table prefix
   */
  public TableNameByWindowFn(String projectId, String datasetId, String tablePrefix) {
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tablePrefix = tablePrefix;
  }

  /**
   * Make an instance
   *
   * @param projectId Google Cloud project ID
   * @param datasetId BigQuery dataset ID
   * @param tablePrefix BigQuery table prefix
   * @param timeZone time zone
   */
  public TableNameByWindowFn(String projectId, String datasetId, String tablePrefix,
                             DateTimeZone timeZone) {
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tablePrefix = tablePrefix;
    this.timeZone = timeZone;
  }

  /**
   * Return table name based on window's start time
   * @param window window
   * @return table name
   */
  public String apply(BoundedWindow window) {
    String dayString = DateTimeFormat.forPattern("yyyyMMdd")
        .withZone(this.timeZone)
        .print(((IntervalWindow) window).start());
    // TODO: extract this formatting function
    return String.format("%s:%s.%s_%s", projectId, datasetId, tablePrefix, dayString);
  }
}
