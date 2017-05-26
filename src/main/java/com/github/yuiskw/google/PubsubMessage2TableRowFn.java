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

import java.util.Arrays;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * This class is used for converting a Pub/Sub message to a BigQuery table row.
 */
public class PubsubMessage2TableRowFn extends DoFn<String, TableRow> {

  static final String TIMESTAMP_COLUMN_NAME = "pubsub_timestamp";
  static final String MESSAGE_COLUMN_NAME = "message";

  static final String datetimePattern = "yyyy-MM-dd HH:mm:ss";
  static final DateTimeFormatter formatter = DateTimeFormat.forPattern(datetimePattern);

  public static String getMessageColumnName() {
    return MESSAGE_COLUMN_NAME;
  }

  public static String getTimestampColumnName() {
    return TIMESTAMP_COLUMN_NAME;
  }

  @DoFn.ProcessElement
  public void processElement(ProcessContext c) {
    DateTime timestamp = c.timestamp().toDateTime();
    String timestampStr = formatter.print(timestamp);

    TableRow row = new TableRow()
        .set(TIMESTAMP_COLUMN_NAME, timestampStr)
        .set(MESSAGE_COLUMN_NAME, c.element());
    c.output(row);
  }

  public static TableSchema getOutputTableSchema() {
    String messageColumn = PubsubMessage2TableRowFn.getMessageColumnName();
    String tsColumn = PubsubMessage2TableRowFn.getTimestampColumnName();
    List<TableFieldSchema> fields = Arrays.asList(
        new TableFieldSchema().setName(messageColumn).setType("STRING"),
        // TODO: Change the data type for timestamp from STRING to DATETIME in BigQuery
        new TableFieldSchema().setName(tsColumn).setType("STRING")
    );
    return new TableSchema().setFields(fields);
  }
}
