/**
 * Copyright(c) http://www.open-v.com
 */
package com.openv.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;

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
public class HDFSAppendDemo {

	/**
	 * * main 方法
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new HdfsConfiguration();
		try {
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/core-site.xml");
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/hdfs-site.xml");
			conf.set("fs.defaultFS", "hdfs://name1:9000");
			FileSystem hdfs = FileSystem.get(conf);
			appendFile(hdfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 文件追加
	 * 
	 * @param hdfs
	 *            FileSystem实例
	 */
	public static void appendFile(FileSystem hdfs) {
		String dst = "/user/baidu/helloWord.txt";
		FileSystem.setDefaultUri(hdfs.getConf(), URI.create(dst));
		FSDataOutputStream fos = null;
		InputStream in = null;
		try {
			fos = hdfs.append(new Path(dst));
			in = new ByteArrayInputStream("welcome to www.openv.org".getBytes());
			IOUtils.copyBytes(in, fos, 1024);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(fos);
		}
	}
}
