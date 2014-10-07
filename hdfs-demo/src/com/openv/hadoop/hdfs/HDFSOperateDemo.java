package com.openv.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * <pre>
 * HDFS文件操作例子。
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
public class HDFSOperateDemo {

	/**
	 * main 方法
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new HdfsConfiguration();
		try {
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/core-site.xml");
			conf.addResource("/home/hadoop/hadoop-2.4.1/etc/hadoop/hdfs-site.xml");
			conf.get("fs.defaultFS");
			conf.getPropertySources("fs.defaultFS");
			conf.set("fs.defaultFS", "hdfs://name1:9000");
			FileSystem hdfs = FileSystem.get(conf);
			if ("create".equals(args[0])) {
				createDir(hdfs);
			} else if ("upload".equals(args[0])) {
				uploadFile(hdfs);
			} else if ("append".equals(args[0])) {
				appendFile(hdfs);
			} else if ("read".equals(args[0])) {
				readFile(hdfs);
			} else if ("list".equals(args[0])) {
				listStatus(hdfs);
			} else if ("remove".equals(args[0])) {
				removeFile(hdfs);
			} else if ("dataNode".equals(args[0])) {
				getAllDataNode(hdfs);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 目录创建
	 * 
	 * @param hdfs
	 *            FileSystem实例
	 */
	public static void createDir(FileSystem hdfs) throws IOException {
		Path path = new Path("/user/baidu");
		boolean flag = hdfs.mkdirs(path);
		System.out.println("make dirs  /user/baidu reslut is  " + flag);
	}

	/**
	 * 文件上传
	 * 
	 * @param hdfs
	 *            FileSystem实例
	 */
	public static void uploadFile(FileSystem hdfs) throws IOException {
		Path source = new Path("/home/hadoop/localFile/helloWord.txt");
		Path source2 = new Path("/home/hadoop/localFile/helloChina.txt");
		Path target = new Path("/user/baidu");
		hdfs.setWorkingDirectory(target);
		hdfs.copyFromLocalFile(source, target);
		hdfs.copyFromLocalFile(source2, target);
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
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(fos);
		}
	}

	/**
	 * 读取文件
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

	/*
	 * 查看文件状态
	 * 
	 * @param hdfs FileSystem实例
	 */
	public static void listStatus(FileSystem hdfs) throws IOException {
		Path path = new Path("/user/baidu/");
		FileStatus[] fst = hdfs.listStatus(path, new PathFilter() {

			@Override
			public boolean accept(Path path) {
				System.out.println("...before filter..." + path.getName());
				if (path.getName().endsWith("helloChina.txt"))
					return false;
				return true;
			}
		});

		Path fspath[] = FileUtil.stat2Paths(fst);
		for (Path path2 : fspath) {
			System.out.println("...after fiter..." + path2);
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

	/*
	 * 查询集群中所有DataNode
	 * 
	 * @param hdfs FileSystem实例
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
