package jnu.tool;
/*
 *MysqlTool provide two methods to  execute sql. two method to value the size of database or table which 
 *you can get the information before you operate on it.
 *
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class MysqlTool {
	private static Logger log = LogManager.getLogger(MysqlTool.class);
	public static Connection getConnection(Configuration conf) {
		String driver = conf.getDriver();
		String url =conf.getUrl();
		String user = conf.getUser();
		String password = conf.getPassword();
		//测试读取是否成功
//		System.out.println(driver);
//		System.out.println(url);
//		System.out.println(user);
//		System.out.println(password);
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

	public static void freeConnection(ResultSet rs, Statement sta, Connection con) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (sta != null) {
				sta.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static ResultSet excuteSql(String sql) {
		Connection con=getConnection(new Configuration());
		Statement stm = null;
		ResultSet res = null;
		try {
			stm = con.createStatement();
			res = stm.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("excuteSql() method run exception");
		}
		return res;
	}
	
	public static void excuteSqlWithoutResult(String sql){
		Connection con=getConnection(new Configuration());
		Statement stmt = null;
		try {
			stmt=con.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.info("excuteSqlWithoutResult() method run exception");
		}
	}
	/*
	 * this method value and return the size of database. 
	 */
	public static int valueDataSize(String dataBaseName) throws SQLException {
		int dataSize = -1;
		String sql = "SELECT truncate(sum(data_length)/1024/1024,2) as data_size FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA="
				+ "'" + dataBaseName + "'";
		ResultSet res=excuteSql(sql);
		if (res.next()) {
			dataSize = (int) Math.ceil(res.getDouble(1));
		}
		return dataSize;
	}
	/*
	 * this method value and return the size of table. 
	 */
	public static int valueDataSize(String dataBaseName, String tableName) throws SQLException {
		int dataSize = -1;
		// 获取表的大小
		String sql = "SELECT truncate(sum(data_length)/1024/1024,2) as data_size FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA="
				+ "'" + dataBaseName + "'" + " AND TABLE_NAME=" + "'" + tableName + "'";
		ResultSet res=excuteSql(sql);
		if (res.next()) {
			dataSize = (int) Math.ceil(res.getDouble(1));
		}
		return dataSize;
	}
}
