package edu.uchicago.mpcs53013.testPail;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Collections ;
import java.util.Map;
import java.lang.RuntimeException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailStructure;



public class PailSerialize {
	public static class Login {
		public String userName;
		public long loginUnixTime;

		public Login(String _user, long _login) {
			userName = _user;
			loginUnixTime = _login;
		}
	} 
	public static class LoginPailStructure implements PailStructure<Login>{
		public Class<Login> getType() {
			return Login.class;
		}
		public byte[] serialize(Login login) {
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(byteOut);
			byte[] userBytes = login.userName.getBytes();
			try {
				dataOut.writeInt(userBytes.length);
				dataOut.write(userBytes);
				dataOut.writeLong(login.loginUnixTime);
				dataOut.close();
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
			return byteOut.toByteArray();
		}

		public Login deserialize(byte[] serialized) {
			DataInputStream dataIn =
					new DataInputStream(new ByteArrayInputStream(serialized));
			try {
				byte[] userBytes = new byte[dataIn.readInt()];
				dataIn.read(userBytes);
				return new Login(new String(userBytes), dataIn.readLong());
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		}
		public List<String> getTarget(Login object) {
			return Collections.EMPTY_LIST;
		}
		public boolean isValidTarget(String... dirs) {
			return true;
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void writeLogins() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<Login> loginPail = Pail.create(fs,
				"/tmp/logins",
				new LoginPailStructure());
		TypedRecordOutputStream out = loginPail.openWrite();
		out.writeObject(new Login("alex", 1352679231));
		out.writeObject(new Login("bob", 1352674216));
		out.close();
	}
	public static void readLogins() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<Login> loginPail = new Pail<Login>(fs, "/tmp/logins");
		for(Login l : loginPail) {
			System.out.println(l.userName + " " + l.loginUnixTime);
		}
	}
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	public static void main(String[] args) {
		if(hadoopPrefix == null) {
			throw new RuntimeException("Please set HADOOP_PREFIX environment variable");
		}
		try {
			if(args[0].equals("s")) {
				writeLogins();
			} else {
				readLogins();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
