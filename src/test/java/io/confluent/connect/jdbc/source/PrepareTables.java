package io.confluent.connect.jdbc.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PrepareTables {
  private static final Logger log = LoggerFactory.getLogger(PrepareTables.class);
  public static final String TOPIC_PREFIX = "test-";

  public static final String T1_START_TIMESTAMP    = "2020-03-05 00:00:0";
  public static final String LOAD_STATUS_TEST_1_TS = "2020-03-05 00:00:00";

  public static final Long T3_START_ID              = 0L;
  public static final Long LOAD_STATUS_TEST_1_LONG  = 0L;
  public static final Long LOAD_STATUS_TEST_2_LONG  = 2L;
  public static final Long LOAD_STATUS_TEST_3_LONG  = 1L;

  public static final String LOAD_STATUS_TEST_TS_NOT_EXISTS = "2030-03-05 00:00:00";

  // -------------------------------------------
  // Offset Table Row
  // -------------------------------------------
  public static class OffsetTableRow {
    public final String topicPrefix;
    public final String srcTableName;
    public final Timestamp lastCompletedTs;
    public final Integer lastCompletedId;

    public OffsetTableRow(ResultSet rs) {
      String    tmpTopicPrefix = null;
      String    tmpSrcTableName = null;
      Timestamp tmpLastCompletedTs = null;
      Integer   tmpLastCompletedId = null;
      try {
        tmpTopicPrefix = rs.getString(1);
        tmpSrcTableName = rs.getString(2);
        tmpLastCompletedTs = rs.getTimestamp(3);
        if (rs.wasNull()) {
          tmpLastCompletedTs = null;
        }
        tmpLastCompletedId = rs.getInt(4);
        if (rs.wasNull()) {
          tmpLastCompletedId = null;
        }
      } catch (SQLException e) {
        log.error("OffsetTableRow: Exception creating the row!", e);
      }
      topicPrefix     = tmpTopicPrefix;
      srcTableName    = tmpSrcTableName;
      lastCompletedTs = tmpLastCompletedTs;
      lastCompletedId = tmpLastCompletedId;
    }

    @Override
    public String toString() {
      return "OffsetTableRow(" +
              "topicPrefix='" + topicPrefix + '\'' +
              ", srcTableName='" + srcTableName + '\'' +
              ", lastCompletedTs=" + lastCompletedTs +
              ", lastCompletedId=" + lastCompletedId +
              ')';
    }
  }
  // -------------------------------------------
  // /Offset Table Row
  // -------------------------------------------
  public static String TABLE_SCHEMA = "DFOCUSVW";
  public static final String TABLE_OFFSET_TABLE_NAME = TABLE_SCHEMA + ".offsets_table";

  private static final String TABLE_OFFSET_TABLE =
          "CREATE TABLE %s (\n"
          + "    topic_prefix varchar(100) NOT NULL,\n"
          + "    table_name varchar(100) NOT NULL,\n"
          + "    last_completed_ts timestamp,\n"
          + "    last_completed_id INTEGER,\n"
          + "    insert_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
          + "    update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
          + "    PRIMARY KEY (topic_prefix, table_name)\n"
          + ")";

  // SELECT COL_LAST_COMPLETED_TS FROM %s WHERE COL_TOPIC_PREFIX = '%s' AND TABLE_NAME = '%s'
  public static final String TABLE_META_LOAD_STATUS_NAME = TABLE_SCHEMA + ".META_LOAD_STATUS";
  private static final String TABLE_META_LOAD_STATUS = "CREATE TABLE " +
          TABLE_META_LOAD_STATUS_NAME + " (\n" +
          "    DB_Table_Nm varchar(200) NOT NULL,\n" +
          "    Table_Max_Td_Update_Ts timestamp,\n" +
          "    Table_Max_Td_Update_Id INTEGER,\n" +
          "    insert_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
          "    update_ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
          "    PRIMARY KEY (DB_Table_Nm)\n" +
          ")\n";

  public static final String TABLE_T1_NAME = TABLE_SCHEMA + ".T1";
  private static final String TABLE_T1 ="CREATE TABLE " + TABLE_T1_NAME + "(\n" +
          "    COL1 VARCHAR(100) NOT null,\n" +
          "    TD_UPDATE_TS timestamp NOT null,\n" +
          "    PRIMARY KEY (col1)\n" +
          "    )\n";

  public static final String TABLE_T2_NAME = TABLE_SCHEMA + ".T2";
  private static final String TABLE_T2 ="CREATE TABLE " + TABLE_T2_NAME + "(\n" +
          "    COL1 VARCHAR(100) NOT null,\n" +
          "    TD_UPDATE_TS timestamp NOT null,\n" +
          "    PRIMARY KEY (col1)\n" +
          "    )";

  public static final String TABLE_T3_NAME = TABLE_SCHEMA + ".T3";
  private static final String TABLE_T3 ="CREATE TABLE " + TABLE_T3_NAME + "(\n" +
          "    COL1 VARCHAR(100) NOT NULL,\n" +
          "    OFFSET_ID INTEGER,\n" +
          "    PRIMARY KEY (col1)\n" +
          "    )";


  private final EmbeddedDerby db;
  private String offsetTableName;

  // -------------------------------------------
  // -- constructor
  // -------------------------------------------
  public PrepareTables(EmbeddedDerby db, String offsetTableName) {
    this.db = db;
    this.offsetTableName = offsetTableName;
    createTables();
  }

  private void createTables() {
    Statement stmt = null;
    try {
      // create schema
      db.execute("CREATE SCHEMA " + TABLE_SCHEMA);
      db.execute("SET SCHEMA " + TABLE_SCHEMA);

      // create tables
      String sql = String.format(TABLE_OFFSET_TABLE, offsetTableName);
      stmt = db.getConnection().createStatement();
      stmt.execute(sql);
      stmt.execute(TABLE_META_LOAD_STATUS);
      stmt.execute(TABLE_T1);
      stmt.execute(TABLE_T2);
      stmt.execute(TABLE_T3);
      //db.createTable(offsetTableName, offsetTableFields);
    } catch (SQLException e) {
      log.error("createTables: Exception creating offset-table!", e);
    } finally {
      closeResources(stmt, null);
    }

  }

  // -------------------------------------------
  // -- closeResources
  // -------------------------------------------
  public void closeResources(Statement stmt, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
    } catch (SQLException e) {
      log.error("getOffsetTable: Exception in closing stmt/rs!", e);
    }
  }

  // ------------------------------------------------------
  // -- getRows
  // -- Requirements:
  //    - className should have a constructor(ResultSet rs)
  // ------------------------------------------------------
  public List<Object> getRows(String className, String sql) {
    Statement stmt = null;
    ResultSet rs = null;
    List<Object> rows = new ArrayList<>();

    Class<?> cl = null;
    try {
      cl = Class.forName(className);
      Constructor<?> cons = cl.getConstructor(ResultSet.class);
      stmt = db.getConnection().createStatement();
      rs = stmt.executeQuery(sql);
      while(rs.next()) {
        rows.add(cons.newInstance(rs));
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResources(stmt, rs);
    }

    return rows;
  }

  public List<OffsetTableRow> getOffsetTableRows(String sql) {
    String className = "io.confluent.connect.jdbc.source.PrepareTables$OffsetTableRow";
    List<Object> l = getRows(className, sql);
    List<OffsetTableRow> rows = new ArrayList<>();

    for (Object o: l) {
      if (o instanceof OffsetTableRow) {
        rows.add((OffsetTableRow)o);
      }
    }

    return rows;
  }

  // -------------------------------------------
  // -- insertIntoOffsetTable
  // -------------------------------------------
  private void insertIntoOffsetTable(String topicPrefix, String tableName,
                                 String lastCompletedTs, Long lastCompletedId) {
    String template = "INSERT INTO " + offsetTableName + "(TOPIC_PREFIX, TABLE_NAME, LAST_COMPLETED_TS, " +
            "LAST_COMPLETED_ID)"
     + " VALUES('%s', '%s', %s, %d)";
    String ts = (lastCompletedTs == null) ? "null" : "'" + lastCompletedTs + "'";
    String sql = String.format(template, topicPrefix, tableName, ts, lastCompletedId);

    try {
      db.getConnection().createStatement().execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // -------------------------------------------
  // -- insertIntoLoadStatusTable
  // -------------------------------------------
  private void insertIntoLoadStatusTable(String DB_Table_Nm,
                                         String Table_Max_Td_Update_Ts,
                                 Long Table_Max_Td_Update_Id) {
    String template = "INSERT INTO " + TABLE_META_LOAD_STATUS_NAME + "(DB_Table_Nm, "
            + "Table_Max_Td_Update_Ts, Table_Max_Td_Update_Id)" + " VALUES('%s', %s, %d)";

    String ts = (Table_Max_Td_Update_Ts == null) ? "null" : "'" + Table_Max_Td_Update_Ts + "'";
    String sql = String.format(template, DB_Table_Nm, ts, Table_Max_Td_Update_Id);
    try {
      db.getConnection().createStatement().execute(sql);
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // -------------------------------------------
  // -- insertIntoT1
  // -------------------------------------------
  private void insertIntoT1(String col1, String ts, int rows) {
    String template = "INSERT INTO " + TABLE_T1_NAME + "(COL1, TD_UPDATE_TS) VALUES('%s', '%s')";
    //String sql = String.format(template, col1, ts);
    Statement stmt = null;
    try {
      stmt = db.getConnection().createStatement();
      int j = 0;
      for (int i=0; i<rows; i++) {
        if (i>0 && i%3 == 0) {
          j++;
        }
        String sql = String.format(template, col1 + (i + 1), ts + j);
        stmt.execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResources(stmt, null);
    }
  }

  // -------------------------------------------
  // -- insertIntoT3
  // -------------------------------------------
  private void insertIntoT3(String col1, Long id, int rows) {
    String template = "INSERT INTO " + TABLE_T3_NAME + "(COL1, OFFSET_ID) VALUES('%s', %s)";
    //String sql = String.format(template, col1, ts);
    Statement stmt = null;
    try {
      stmt = db.getConnection().createStatement();
      int j = 0;
      for (int i=0; i<rows; i++) {
        if (i>0 && i%3 == 0) {
          j++;
        }
        String sql = String.format(template, col1 + (i + 1), id + j);
        stmt.execute(sql);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResources(stmt, null);
    }
  }


  // -------------------------------------------
  // -- prepareForTimestampTest_1
  // -------------------------------------------
  public void prepareForTimestampTest_1() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T1_NAME,
            LOAD_STATUS_TEST_1_TS, null);

    insertIntoT1("x", T1_START_TIMESTAMP,  3);
  }

  // -------------------------------------------
  // -- prepareForIdTest_1
  // -------------------------------------------
  public void prepareForIdTest_1() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T3_NAME,
            null, LOAD_STATUS_TEST_1_LONG);

    insertIntoT3("x", T3_START_ID,  3);
  }

  // -------------------------------------------
  // -- prepareForIdTest_2
  // -------------------------------------------
  public void prepareForIdTest_2() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T3_NAME,
            null, LOAD_STATUS_TEST_2_LONG);

    insertIntoT3("x", T3_START_ID,  7);
  }

  // -------------------------------------------
  // -- prepareForIdTest_3
  // -------------------------------------------
  public void prepareForIdTest_3() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T3_NAME,
            null, LOAD_STATUS_TEST_3_LONG);

    insertIntoT3("x", T3_START_ID,  7);
  }

  // -------------------------------------------
  // -- prepareForTimestampTest_1
  // -------------------------------------------
  public void prepareForBothTsAndLongOffsetNotNull() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T1_NAME,
            LOAD_STATUS_TEST_1_TS, null);

    insertIntoT1("x", T1_START_TIMESTAMP,  3);
    insertIntoOffsetTable(TOPIC_PREFIX, TABLE_T1_NAME, LOAD_STATUS_TEST_1_TS,
            LOAD_STATUS_TEST_1_LONG);
  }

  // -------------------------------------------
  // -- prepareForUpperBoundNotExistsInTable
  // -------------------------------------------
  public void prepareForUpperBoundNotExistsInTable() {
    // insert into load status table
    insertIntoLoadStatusTable(TABLE_T1_NAME,
            LOAD_STATUS_TEST_TS_NOT_EXISTS, null);

    insertIntoT1("x", T1_START_TIMESTAMP,  3);
  }

  // -------------------------------------------
  // -- prepareForNoRun
  // -------------------------------------------
  public void prepareForNoRun() {
    insertIntoT1("x", T1_START_TIMESTAMP,  3);
  }


  public Timestamp getLastOffsetTSforTable(String topicPrefix, String tableName) {
    String sql =
            String.format("SELECT LAST_COMPLETED_TS FROM %s WHERE "
                    + "TOPIC_PREFIX = '%s' AND TABLE_NAME = '%s'",
                    TABLE_OFFSET_TABLE_NAME, topicPrefix, tableName);
    Timestamp ts = null;
    try {
      ResultSet rs = db.getConnection().createStatement().executeQuery(sql);
      if (rs.next()) {
        ts = rs.getTimestamp(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return ts;
  }

  public Long getLastOffsetLongforTable(String topicPrefix, String tableName) {
    String sql =
            String.format("SELECT LAST_COMPLETED_ID FROM %s WHERE "
                            + "TOPIC_PREFIX = '%s' AND TABLE_NAME = '%s'",
                    TABLE_OFFSET_TABLE_NAME, topicPrefix, tableName);
    Long id = null;
    try {
      ResultSet rs = db.getConnection().createStatement().executeQuery(sql);
      if (rs.next()) {
        id = rs.getLong(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return id;
  }

  // -------------------------------------------
  // -- main
  // -------------------------------------------
  public static void main(String[] args) {
    EmbeddedDerby db  =new EmbeddedDerby();
    PrepareTables pt = new PrepareTables(db, TABLE_OFFSET_TABLE_NAME);
    pt.prepareForTimestampTest_1();
    try {
      db.close();
      db.dropDatabase();
    } catch (SQLException | IOException e) {
      e.printStackTrace();
    }
  }
}
