package com.salesforce.jirachi.ingest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.salesforce.phoenix.schema.TableAlreadyExistsException;

public class PhoenixUtils {
  
  private static String zkQuorum = "localhost"; // TODO: pull from configs

  public static Connection getPhoenixConnection() throws Exception {
    Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
    String connectionURL = "jdbc:phoenix:" + zkQuorum;
    Connection r = DriverManager.getConnection (connectionURL);
    r.setAutoCommit(true);
    return r;
  }
  
  public static void execute(Connection conn, String SQL) throws Exception {
    Statement stmt = conn.createStatement();
    try {
      stmt.execute(SQL.toString());
    } catch (Exception ex){
      System.out.println("Exception running SQL: " + SQL.toString());
      throw ex;
    }
  }

  /**
   * Execute a SQL DDL statement and ignore any "table already exists" errors
   */
  public static void executeNoTableExistsThrow(Connection conn, String SQL) throws Exception {
    try {
      execute(conn,SQL);
    } catch (TableAlreadyExistsException ex) {
      // GULP
    }
  }
}
