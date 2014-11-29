/**
 * Copyright(c) http://www.open-v.com
 */
package com.openv.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
public class HDFSReadDemo {
	
	public static void main(String[] args) {
		Configuration conf = new HdfsConfiguration();
		try {
			conf.addResource("/home/hadoop/hadoop-2.5.1/etc/hadoop/core-site.xml");
			conf.addResource("/home/hadoop/hadoop-2.5.1/etc/hadoop/hdfs-site.xml");
			conf.set("fs.defaultFS", "hdfs://name1:9000");
			FileSystem hdfs = FileSystem.get(conf);
			readFile(hdfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件。
	 * 
	 * @param hdfs
	 *            FileSystem实例
	 */
	public static void readFile(FileSystem hdfs) {
		String src = "/user/baidu/helloWord.txt";
		FileSystem.setDefaultUri(hdfs.getConf(), URI.create(src));
		OutputStream out = System.out;
		FSDataInputStream fis = null;
		try {
			fis = hdfs.open(new Path(src));
			IOUtils.copyBytes(fis, out, 1024);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fis);
			IOUtils.closeStream(out);
		}
	}

}
