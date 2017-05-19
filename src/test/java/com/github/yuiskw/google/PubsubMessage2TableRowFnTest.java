package com.github.yuiskw.google;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.codehaus.jackson.map.ObjectMapper;

public class PubsubMessage2TableRowFnTest {

  @Test
  public void testApply() {
    ObjectMapper mapperObj = new ObjectMapper();
    Map map = new HashMap();
    map.put("foo", 1);
    map.put("bar", Arrays.asList(1, 2, 3));
    Map submap = new HashMap();
    submap.put("moge", 1);
    map.put("hoge", submap);
    String jsonStr = new Gson().toJson(map);

    PubsubMessage2TableRowFn fn = new PubsubMessage2TableRowFn();
    DoFnTester<String, TableRow> fnTester = DoFnTester.of(fn);

    try {
      List<TableRow> result = fnTester.processBundle(jsonStr);
      String msg = result.get(0).get(PubsubMessage2TableRowFn.getMessageColumnName()).toString();
      assertEquals(jsonStr, msg);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}