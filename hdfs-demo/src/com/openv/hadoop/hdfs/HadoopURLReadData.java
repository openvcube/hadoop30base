package com.openv.hadoop.hdfs;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

/**
 * @author http://www.openv.org
 */
public class HadoopURLReadData {
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) {
		InputStream in=null;
		try {
			in=new URL("hdfs://192.168.0.130:9000/user/openv/at.txt").openStream();
			IOUtils.copy(in, System.out);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeQuietly(in);
		}
	}
}
