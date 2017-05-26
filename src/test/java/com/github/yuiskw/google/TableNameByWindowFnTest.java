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