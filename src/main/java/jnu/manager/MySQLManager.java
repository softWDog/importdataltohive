package jnu.manager;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.fs.FSDataOutputStream;



import jline.internal.Log;
import jnu.tool.HDFSTool;
import jnu.tool.HiveTool;
import jnu.tool.MysqlTool;

public class MySQLManager {
public static void main(String[] args) throws SQLException, IOException {
	String sql="select * from student";
	Log.info("连接mysql，开始导出数据...");
	ResultSet res = MysqlTool.excuteSql(sql);
	ResultSetMetaData resultSetMetaData = res.getMetaData();
	int numberOfColumns = resultSetMetaData.getColumnCount();
	int numberOfRows = res.getRow();
	String dst = "\\student";
	FSDataOutputStream outputStream = HDFSTool.fileOutputStream(dst);
	Log.info("导入hdfs");
	long startTime=System.currentTimeMillis();
	while (res.next()) {
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i <= numberOfColumns; i++) {
			sb.append(res.getString(i) + "\t");
		}
		sb.append("\n");
		// System.out.println(sb.toString());
		outputStream.write(sb.toString().getBytes("UTF-8"));
	}
	sql="load data inpath '/student' into table student";
	HiveTool.excuteSqlWithoutResult(sql);
	long endTime=System.currentTimeMillis();
	//运行了多少时间
	Log.info("running "+String.valueOf((endTime-startTime)/1000)+"second");
	Log.info("导入hive成功");
}
}
