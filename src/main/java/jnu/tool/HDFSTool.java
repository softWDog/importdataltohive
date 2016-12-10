package jnu.tool;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HDFSTool {
	private static Logger log=LogManager.getLogger(HDFSTool.class);
	public static FileSystem getFileSystem(){
		Configuration conf = new Configuration();
		FileSystem fs=null;
		try {
			fs = FileSystem.get(URI.create("hdfs://192.168.1.21:9000"),conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.info("Connection refused,please check your hadoop enviroment");
		}
		return fs;
	}
	public static FSDataOutputStream fileOutputStream(String dst) throws IOException {
		FileSystem fs=getFileSystem();
		Path dstPath = new Path(dst); 
		FSDataOutputStream outputStream = fs.create(dstPath);
		return outputStream;
	}

	
	public static void uploadFile(String src, String dst) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(src); 
		Path dstPath = new Path(dst); 
		fs.copyFromLocalFile(false, srcPath, dstPath);
		System.out.println("Upload to " + conf.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fs.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fs.close();
	}

	
	public static void rename(String oldName, String newName) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path oldPath = new Path(oldName);
		Path newPath = new Path(newName);
		boolean isok = fs.rename(oldPath, newPath);
		if (isok) {
			System.out.println("rename ok!");
		} else {
			System.out.println("rename failure");
		}
		fs.close();
	}

	
	public static void delete(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		boolean isok = fs.deleteOnExit(path);
		if (isok) {
			System.out.println("delete ok!");
		} else {
			System.out.println("delete failure");
		}
		fs.close();
	}

	
	public static void mkdir(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok!");
		} else {
			System.out.println("create dir failure");
		}
		fs.close();
	}

	
	public static void readFile(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(filePath);
		InputStream in = null;
		try {
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, System.out, 4096, false); // 复制到标准输出流
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
