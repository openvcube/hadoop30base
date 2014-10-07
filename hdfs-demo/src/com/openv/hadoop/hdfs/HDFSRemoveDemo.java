/**
 * Copyright(c) http://www.open-v.com
 */
package com.openv.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

/**
 * <pre>
 * 程序的中文名称。
 * </pre>
 * 
 * @author http://www.open-v.com
 * @version 1.00.00
 * 
 *          <pre>
 * 修改记录
 *    修改后版本:     修改人：  修改日期:     修改内容:
 * </pre>
 */
public class HDFSRemoveDemo {

	public static void main(String[] args) {
		Configuration conf = new HdfsConfiguration();
		try {
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/core-site.xml");
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/hdfs-site.xml");
			conf.set("fs.defaultFS", "hdfs://name1:9000");
			FileSystem hdfs = FileSystem.get(conf);
			removeFile(hdfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 文件删除
	 * 
	 * @param hdfs FileSystem实例
	 */
	public static void removeFile(FileSystem hdfs) throws IOException {
		Path path = new Path("/user/baidu/helloChina.txt");
		Path path2 = new Path("/user/baidu/helloWord.txt");
		hdfs.deleteOnExit(path);
		hdfs.deleteOnExit(path2);
	}
}
