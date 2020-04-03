package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static io.confluent.connect.jdbc.source.PrepareTables.TABLE_T1_NAME;
import static io.confluent.connect.jdbc.source.PrepareTables.TABLE_T3_NAME;
import static org.junit.Assert.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest({BatchIdTableQuerier.class})
@PowerMockIgnore("javax.management.*")
public class BatchIdTableQuerierConfigTest extends JdbcSourceTaskTestBase {
  protected static String TIMESTAMP_COLUMN_NAME = "TD_UPDATE_TS";
  protected static String TIMESTAMP_COLUMN_NAMES = "TD_UPDATE_TS, TD_UPDATE_TS2";
  protected static String INCREMENTAL_COLUMN_NAME = "OFFSET_ID";
  protected static String QUERY = "SELECT * FROM T1";

  protected static final String BATCH_PRE_QUERY_TS =
          "SELECT Table_Max_TD_UPDATE_TS FROM DFOCUSVW.META_LOAD_STATUS WHERE DB_Table_Nm = "
                  + "__TABLE_NAME__ AND Table_Max_TD_UPDATE_TS > __OFFSET__";
  protected static final String BATCH_PRE_QUERY_LONG =
          "SELECT Table_Max_Td_Update_Id FROM DFOCUSVW.META_LOAD_STATUS WHERE DB_Table_Nm = "
                  + "__TABLE_NAME__ AND Table_Max_Td_Update_Id > __OFFSET__";

  // protected static final String BATCH_DEFAULT_OFFSET_START_TS = "1600-01-01 00:00:00";
  protected static final String BATCH_DEFAULT_OFFSET_START_TS = "2020-03-05 00:00:00";
  protected static final String BATCH_DEFAULT_OFFSET_START_LONG = "0";


  protected Map<String, String> singleTableConfigBatchMode(boolean timestamp) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());

    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BATCH_ID);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    props.put(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG, "true");

    props.put(JdbcSourceTaskConfig.BATCH_OFFSETS_STORAGE_CONFIG, PrepareTables.TABLE_OFFSET_TABLE_NAME);
    props.put(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG, "false");
    props.put(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG, "100");
    props.put(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG, "10000");
    props.put(JdbcSourceTaskConfig.DB_TIMEZONE_CONFIG, "UTC");

    if (timestamp) {
      props.put(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME);
      props.put(JdbcSourceConnectorConfig.BATCH_QUERY_PRE_RUN_CHECK_CONFIG, BATCH_PRE_QUERY_TS);
      props.put(JdbcSourceConnectorConfig.BATCH_DEFAULT_OFFSET_START_CONFIG, BATCH_DEFAULT_OFFSET_START_TS);
      props.put(JdbcSourceTaskConfig.TABLES_CONFIG, TABLE_T1_NAME);
      props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, TABLE_T1_NAME);
    } else {
      props.put(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG, INCREMENTAL_COLUMN_NAME);
      props.put(JdbcSourceConnectorConfig.BATCH_QUERY_PRE_RUN_CHECK_CONFIG, BATCH_PRE_QUERY_LONG);
      props.put(JdbcSourceConnectorConfig.BATCH_DEFAULT_OFFSET_START_CONFIG, BATCH_DEFAULT_OFFSET_START_LONG);
      props.put(JdbcSourceTaskConfig.TABLES_CONFIG, TABLE_T3_NAME);
      props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, TABLE_T3_NAME);
    }
    return props;
  }

  // -------------------------------------------------------------
  // This tests contain all the configuration contract violations
  // -------------------------------------------------------------
  // -------------------------------------------
  // -- You cannot specify both timestamp and
  // -- incremental columns
  // -------------------------------------------
  @Test // ConnectException
  public void testBothIncrementAndTimestampColumns() {
    try {
      Map<String, String> props = singleTableConfigBatchMode(true);
      props.put(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG, INCREMENTAL_COLUMN_NAME);
      task.start(props);
      fail("Should have thrown ConnectException but did not!");
    } catch (ConnectException e) {
      assertEquals(BatchIdTableQuerier.ERR_CONF_BOTH_TS_INC_COLUMN_USED, e.getMessage());
    }
  }

  // -------------------------------------------
  // -- No timestamp or incremental column are
  // -- provided
  // -------------------------------------------
  @Test // ConnectException
  public void testNeitherIncrementAndTimestampColumns() {
    try {
      Map<String, String> props = singleTableConfigBatchMode(true);
      props.remove(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
      task.start(props);
      fail("Should have thrown ConnectException but did not!");
    } catch (ConnectException e) {
      assertEquals(BatchIdTableQuerier.ERR_CONF_NO_TS_OR_INC_COLUMN, e.getMessage());
    }
  }

  // -------------------------------------------
  // -- multiple timestamp column are provided
  // -------------------------------------------
  @Test // ConnectException
  public void testMultipeTimestampColumn() {
    try {
      Map<String, String> props = singleTableConfigBatchMode(true);
      props.put(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAMES);
      task.start(props);
      fail("Should have thrown ConnectException but did not!");
    } catch (ConnectException e) {
      assertEquals(BatchIdTableQuerier.ERR_CONF_MULTIPLE_TS_COLUMNS, e.getMessage());
    }
  }

  // -------------------------------------------
  // -- query config parameter is specified
  // -------------------------------------------
  @Test // ConnectException
  public void testQueryInConfig() {
    try {
      Map<String, String> props = singleTableConfigBatchMode(true);
      props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
      props.put(JdbcSourceTaskConfig.QUERY_CONFIG, QUERY);
      task.start(props);
      fail("Should have thrown ConnectException but did not!");
    } catch (ConnectException e) {
      assertEquals(BatchIdTableQuerier.ERR_CONF_QUERY_MODE_NOT_SUPPORTED, e.getMessage());
    }
  }

  // -------------------------------------------
  // -- offset table contains both timestamp and
  // -- incremental offset for a table
  // -------------------------------------------
  @Test // ConnectException
  public void testBothTsAndLongOffsetNotNull() throws InterruptedException {
    try {
      // set process time to UTC
      System.setProperty("user.timezone", "UTC");
      TimeZone.setDefault(null);

      // prepare db
      PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
      pt.prepareForBothTsAndLongOffsetNotNull();

      // start task
      Map<String, String> props = singleTableConfigBatchMode(true);
      BatchIdTableQuerier.DEBUG = true;
      task.start(props);

    } catch (ConnectException e) {
      assertEquals(e.getMessage(), JdbcBatchIdManager.ERR_CONF_BOTH_TS_AND_LONG_OFFSET_NOT_NULL);
    }
  }

  // -------------------------------------------
  // -- The uppoer bound returned by the
  // -- pre-query does not exist in the table
  // -------------------------------------------
  @Test // ConnectException
  public void testUpperBoundNotExistsInTable() throws InterruptedException {
    try {
      // set process time to UTC
      System.setProperty("user.timezone", "UTC");
      TimeZone.setDefault(null);

      // prepare db
      PrepareTables pt = new PrepareTables(db, PrepareTables.TABLE_OFFSET_TABLE_NAME);
      pt.prepareForUpperBoundNotExistsInTable();

      // start task
      Map<String, String> props = singleTableConfigBatchMode(true);
      BatchIdTableQuerier.DEBUG = true;
      task.start(props);

      // poll
      List<SourceRecord> records = task.poll();
    } catch (ConnectException e) {
      assertTrue(e.getMessage().contains(
              BatchIdTableQuerier.ERR_CONF_LOAD_STATUS_UPPER_BOUND_DOES_NOT_EXIST));
    }
  }


}
