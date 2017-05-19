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
