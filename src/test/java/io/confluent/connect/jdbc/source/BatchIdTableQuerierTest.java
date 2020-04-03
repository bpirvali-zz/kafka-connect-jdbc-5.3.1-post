package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static io.confluent.connect.jdbc.source.PrepareTables.TABLE_T1_NAME;
import static io.confluent.connect.jdbc.source.PrepareTables.TABLE_T3_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchIdTableQuerier.class})
@PowerMockIgnore("javax.management.*")
public class BatchIdTableQuerierTest extends BatchIdTableQuerierConfigTest  {

  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    db = new EmbeddedDerby();
    BatchIdTableQuerier.offsets.clear();
    JdbcBatchIdManager.offsets.clear();
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testPreQueryNotRun()  {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare db
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForNoRun();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(true);
    BatchIdTableQuerier.DEBUG = true;
    task.start(props);

    // Do pre-processing
    boolean run = task.getTableQuerier().doPreProcessing();

    assertEquals(false, run);
  }

  @Test
  public void testPreQueryRun()  {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare db
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForTimestampTest_1();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(true);
    BatchIdTableQuerier.DEBUG = true;
    task.start(props);

    // Do pre-processing
    boolean run = task.getTableQuerier().doPreProcessing();

    assertEquals(true, run);
  }

  @Test
  public void testTimestampBatchMode() throws InterruptedException {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare db
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForTimestampTest_1();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(true);
    BatchIdTableQuerier.DEBUG = true;
    JdbcBatchIdManager.DEBUG = true;
    task.start(props);

    // poll
    List<SourceRecord> records = task.poll();

    // verify offsets
    assertEquals(3, records.size());
    // assertEquals("1599-12-31 23:59:59.999 | 2020-03-05 00:00:00.0", BatchIdTableQuerier.offsets.get(0));
    assertEquals("2020-03-04 23:59:59.999 | 2020-03-05 00:00:00.0", BatchIdTableQuerier.offsets.get(0));
    assertEquals("2020-03-05 00:00:00.0 | 2020-03-05 00:00:00.0", BatchIdTableQuerier.offsets.get(1));
    assertEquals("2020-03-05 00:00:00.0 | 2020-03-05 00:00:00.0", BatchIdTableQuerier.offsets.get(2));

    // verify offset changes
    // assertEquals("1599-12-31 23:59:59.999", JdbcBatchIdManager.offsets.get(0));
    assertEquals("2020-03-04 23:59:59.999", JdbcBatchIdManager.offsets.get(0));
    assertEquals("2020-03-05 00:00:00.0", JdbcBatchIdManager.offsets.get(1));

    // Verify runto-offset with offset int the offset table
    Timestamp ts = pt.getLastOffsetTSforTable(TOPIC_PREFIX, TABLE_T1_NAME);
    assertEquals(PrepareTables.LOAD_STATUS_TEST_1_TS + ".0", ts.toString());
  }

  @Test
  public void testIdBatchMode() throws InterruptedException {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare db
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForIdTest_1();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(false);
    BatchIdTableQuerier.DEBUG = true;
    JdbcBatchIdManager.DEBUG = true;
    task.start(props);

    // poll
    List<SourceRecord> records = task.poll();

    // verify offsets
    assertEquals(3, records.size());
    assertEquals("-1 | 0", BatchIdTableQuerier.offsets.get(0));
    assertEquals("0 | 0", BatchIdTableQuerier.offsets.get(1));
    assertEquals("0 | 0", BatchIdTableQuerier.offsets.get(2));

    // verify offset changes
    assertEquals("-1", JdbcBatchIdManager.offsets.get(0));
    assertEquals("0", JdbcBatchIdManager.offsets.get(1));

    // Verify runto-offset with offset int the offset table
    Long id = pt.getLastOffsetLongforTable(TOPIC_PREFIX, TABLE_T3_NAME);
    assertTrue(id.equals(PrepareTables.LOAD_STATUS_TEST_1_LONG));
  }

  @Test
  public void testIdBatchModeMultipleBatches() throws InterruptedException {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare d
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForIdTest_2();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(false);
    BatchIdTableQuerier.DEBUG = true;
    JdbcBatchIdManager.DEBUG = true;
    task.start(props);

    // poll
    List<SourceRecord> records = task.poll();

    // verify offsets
    assertEquals(7, records.size());
    assertEquals("-1 | 0", BatchIdTableQuerier.offsets.get(0));
    assertEquals("0 | 0", BatchIdTableQuerier.offsets.get(1));
    assertEquals("0 | 0", BatchIdTableQuerier.offsets.get(2));
    assertEquals("0 | 1", BatchIdTableQuerier.offsets.get(3));
    assertEquals("1 | 1", BatchIdTableQuerier.offsets.get(4));
    assertEquals("1 | 1", BatchIdTableQuerier.offsets.get(5));
    assertEquals("1 | 2", BatchIdTableQuerier.offsets.get(6));

    // verify offset changes
    assertEquals("-1", JdbcBatchIdManager.offsets.get(0));
    assertEquals("0", JdbcBatchIdManager.offsets.get(1));
    assertEquals("1", JdbcBatchIdManager.offsets.get(2));
    assertEquals("2", JdbcBatchIdManager.offsets.get(3));

    // Verify runto-offset == offset int the offset table
    Long id = pt.getLastOffsetLongforTable(TOPIC_PREFIX, TABLE_T3_NAME);
    assertTrue(id.equals(PrepareTables.LOAD_STATUS_TEST_2_LONG));
  }

  @Test
  public void testIdBatchModeRetrieveRangeOfBatches() throws InterruptedException {
    // set process time to UTC
    System.setProperty("user.timezone", "UTC");
    TimeZone.setDefault(null);

    // prepare d
    PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    pt.prepareForIdTest_3();

    // start task
    Map<String, String> props = singleTableConfigBatchMode(false);
    props.put(JdbcSourceConnectorConfig.BATCH_DEFAULT_OFFSET_START_CONFIG, "1");
    BatchIdTableQuerier.DEBUG = true;
    JdbcBatchIdManager.DEBUG = true;
    task.start(props);

    // poll
    List<SourceRecord> records = task.poll();

    // verify offsets
    assertEquals(3, records.size());
    assertEquals("0 | 1", BatchIdTableQuerier.offsets.get(0));
    assertEquals("1 | 1", BatchIdTableQuerier.offsets.get(1));
    assertEquals("1 | 1", BatchIdTableQuerier.offsets.get(2));

    // verify offset changes
    assertEquals("0", JdbcBatchIdManager.offsets.get(0));
    assertEquals("1", JdbcBatchIdManager.offsets.get(1));

    // Verify runto-offset == offset int the offset table
    Long id = pt.getLastOffsetLongforTable(TOPIC_PREFIX, TABLE_T3_NAME);
    assertTrue(id.equals(PrepareTables.LOAD_STATUS_TEST_3_LONG));
  }
}



