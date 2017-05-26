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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class LogAggregatorTest {

  public static String readStream(InputStream is) {
    StringBuilder sb = new StringBuilder(512);
    try {
      Reader r = new InputStreamReader(is, "UTF-8");
      int c = 0;
      while ((c = r.read()) != -1) {
        sb.append((char) c);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetOptionsWithSetting() {
    String[] args = {
        "--project=test_project"
        , "--outputBigQueryDataset=test_dataset"
        , "--outputBigQueryTable=test_table"
        , "--inputPubsubSubscription=test_subscription"
    };
    LogAggregator.Options options = LogAggregator.getOptions(args);
    assertEquals("test_project", options.getProject());
    assertEquals("test_dataset", options.getOutputBigQueryDataset());
    assertEquals("test_table", options.getOutputBigQueryTable());
    assertEquals("test_subscription", options.getInputPubsubSubscription());
  }

  @Test
  public void testTransformer() {
    // Read file
    InputStream inputStream =
        this.getClass().getResourceAsStream("/dummy-log");
    // Convert to JSON string
    String json = readStream(inputStream);

    LogAggregator.LogTransformer transformer = new LogAggregator.LogTransformer();

    List<String> inputs = Arrays.asList(json, json, json);
    PCollection<String> inputCollection = pipeline.apply(Create.of(inputs));
    PCollection<TableRow> outputCollection = inputCollection.apply(transformer);
    PAssert.that(outputCollection).satisfies(new HasColumnsCheckerFn());
    pipeline.run().waitUntilFinish();
  }

  private static class HasColumnsCheckerFn
      implements SerializableFunction<Iterable<TableRow>, Void> {
    public Void apply(Iterable<TableRow> input) {
      for (TableRow tableRow : input) {
        assertTrue(tableRow.size() > 0);
        assertTrue(tableRow.containsKey(PubsubMessage2TableRowFn.TIMESTAMP_COLUMN_NAME));
        assertTrue(tableRow.containsKey(PubsubMessage2TableRowFn.MESSAGE_COLUMN_NAME));
      }
      return null;
    }
  }
}