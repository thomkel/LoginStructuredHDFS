package edu.uchicago.mpcs53013.testPail;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;


public class PailTest {
	public static void simpleIO() throws IOException {
	        Configuration conf = new Configuration();
      	        conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
	        FileSystem fs = FileSystem.get(conf);
		Pail pail = Pail.create(fs, "/tmp/mypail");
		TypedRecordOutputStream os = pail.openWrite();
		os.writeObject(new byte[] {1, 2, 3});
		os.writeObject(new byte[] {1, 2, 3, 4});
		os.writeObject(new byte[] {1, 2, 3, 4, 5});
		os.close();
	}
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(hadoopPrefix == null) {
			throw new RuntimeException("Please set HADOOP_PREFIX environment variable");
		}
		try {
			simpleIO();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}