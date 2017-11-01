
package com.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		Class.forName(driverName);
		// replace "hduser" here with the name of the user the queries should
		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hduser", "");
		Statement stmt = con.createStatement();
		String tableName = "testHiveDriverTable";
		String sql;
		ResultSet res;

		// Drop Table
		dropTable(stmt, tableName);

		// Create table
		createTable(stmt, tableName);

		// show tables
		showTable(stmt, tableName);

		// describe table
		describeTable(stmt, tableName);

		// load data into table
		loadData(stmt, tableName);

		// select * query
		fetchData(stmt, tableName);

		// regular hive query
		rowCount(stmt, tableName);
	}

	private static void createTable(Statement stmt, String tableName) throws SQLException {
		stmt.execute("create table " + tableName
				+ " (key int, value string) row format delimited fields terminated by '\t' ");
	}

	private static void dropTable(Statement stmt, String tableName) throws SQLException {
		stmt.execute("drop table if exists " + tableName);
	}

	private static void rowCount(Statement stmt, String tableName) throws SQLException {
		String sql;
		ResultSet res;
		sql = "select count(*) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	private static void fetchData(Statement stmt, String tableName) throws SQLException {
		String sql;
		ResultSet res;
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}
	}

	private static void loadData(Statement stmt, String tableName) throws SQLException {
		String sql;
		String filepath = "/home/impadmin/Desktop/output.csv";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}

	private static void describeTable(Statement stmt, String tableName) throws SQLException {
		String sql;
		ResultSet res;
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
	}

	private static void showTable(Statement stmt, String tableName) throws SQLException {
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}