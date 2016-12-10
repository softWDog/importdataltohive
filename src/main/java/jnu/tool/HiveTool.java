package jnu.tool;
/*
 * HiveTool is used to get connection from hive server and also provide two method to execute sql.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class HiveTool{
	private static Logger log = LogManager.getLogger(HiveTool.class);

	public static Connection getHiveConnection() {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = null;
		try {
			//It is my hive test url address.
			con = DriverManager.getConnection("jdbc:hive2://192.168.1.21:10000/default", "hadoop", "");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("Get connnection failed,Please check your hive enviroment CHECK");
		}
		return con;
	}

	public static void freeConnection(Connection conn, Statement stmt, ResultSet res) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.info("Hive close exception");
			}
		}
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.info("Hive statement close exception");
			}
		}
		if (res != null) {
			try {
				res.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				log.info("Hive reslut close exception");
			}
		}
	}

	public static ResultSet excuteSql(String sql) {
		Connection con = getHiveConnection();
		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("Get hive statement exception");
		}
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.info("Get hive reslut exception");
		}
		return res;
	}

	public static void excuteSqlWithoutResult(String sql) {
		Connection con = getHiveConnection();
		Statement stmt = null;
		try {
			stmt = con.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("Get hive statement exception");
		}
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("excute hive sql error");
		}

	}
}
