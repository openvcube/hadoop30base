/**
 * Copyright(c) http://www.open-v.com
 */
package com.openv.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

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
public class HDFSGetDataNodesDemo {

	public static void main(String[] args) {
		Configuration conf = new HdfsConfiguration();
		try {
			conf.addResource("/home/hadoop/hadoop-2.5.1/etc/hadoop/core-site.xml");
			conf.addResource("/home/hadoop/hadoop-2.5.1/etc/hadoop/hdfs-site.xml");
			conf.set("fs.defaultFS", "hdfs://name1:9000");
			FileSystem hdfs = FileSystem.get(conf);
			getAllDataNode(hdfs);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 查询集群中所有DataNode。
	 * 
	 * @param hdfs
	 *            FileSystem实例
	 *
	 */
	public static void getAllDataNode(FileSystem hdfs) throws IOException {
		DistributedFileSystem dfs = (DistributedFileSystem) hdfs;
		DatanodeInfo[] nodeArr = dfs.getDataNodeStats();
		if (nodeArr != null && nodeArr.length > 0) {
			for (DatanodeInfo dataNode : nodeArr) {
				System.out.println(dataNode.getHostName() + ":"
						+ dataNode.getIpAddr());
			}
		}
	}

}
