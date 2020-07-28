/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import junit.framework.Assert;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestMergeJoins {

  @BeforeClass
  public static void Setup() {
  }

  @AfterClass
  public static void Teardown() throws Exception {
    Driver driver = createDriver();
    driver.run("drop table test1");
    driver.run("drop table test2");
    driver.run("drop table test3");
  }

  @Test
  public void testQueryTable1() throws IOException {
    Driver driver = createDriver();
    driver.run("create table test1(ida string)").getResponseCode();
    driver.run("insert into test1 select 'id-1000'").getResponseCode();
    driver.run("create table test2(idb string, shop string)").getResponseCode();
    driver.run("insert into test2 select 'id-1000', 'abcde'").getResponseCode();
    driver.run("create table test3(idc bigint, id2 string, id3 string)").getResponseCode();
    CommandProcessorResponse res = driver.run("select a.ida,b.shop from (select * from test1) a\n" +
            "left outer join (select * from test2) b\n" +
            "      ON a.ida=b.idb\n" +
            "left outer join (select * from test3) c\n" +
            "      ON a.ida=c.idc");
    List result = new ArrayList();
    try {
      driver.getResults(result);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
    Assert.assertNotNull(result.get(0));
    Assert.assertEquals("id-1000\tabcde", result.get(0));
  }

  private static Driver createDriver() {
    HiveConf conf = new HiveConf(Driver.class);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_MERGE_NWAY_JOINS, true);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    return driver;
  }

}
