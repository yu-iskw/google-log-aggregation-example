package com.github.yuiskw.google;

import java.util.Formatter;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TableNameByWindowFnTest {

  @Test
  public void testApply() {
    DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    Instant start = format.parseDateTime("2017-01-01 00:00:00").toInstant();
    Instant end = format.parseDateTime("2017-01-01 00:01:00").toInstant();
    IntervalWindow window = new IntervalWindow(start, end);

    String projectId = "testProject_id";
    String datasetId = "testDatasetId";
    String tablePrefix = "testTablePrefix";
    TableNameByWindowFn fn = new TableNameByWindowFn(projectId, datasetId, tablePrefix);
    String result = fn.apply(window);
    String expected = new Formatter()
        .format("%s:%s.%s_%s", projectId, datasetId, tablePrefix, "20170101")
        .toString();
    assertEquals(expected, result);
  }
}