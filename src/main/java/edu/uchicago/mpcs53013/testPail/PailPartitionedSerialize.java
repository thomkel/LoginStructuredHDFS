package edu.uchicago.mpcs53013.testPail;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Collections ;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.hadoop.pail.SequenceFileFormat;

public class PailPartitionedSerialize {
	public static class Login {
		public String userName;
		public long loginUnixTime;

		public Login(String _user, long _login) {
			userName = _user;
			loginUnixTime = _login;
		}
	} 
	public static class LoginPailStructure implements PailStructure<Login>{
		public Class getType() {
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
	public static class PartitionedLoginPailStructure extends LoginPailStructure {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		public List<String> getTarget(Login object) {
			ArrayList<String> directoryPath = new ArrayList<String>();
			Date date = new Date(object.loginUnixTime * 1000L);
			directoryPath.add(formatter.format(date));
			return directoryPath;
		}
		public boolean isValidTarget(String... strings) {
			if(strings.length > 2) return false;
			try {
				return (formatter.parse(strings[0]) != null);
			}
			catch(ParseException e) {
				return false;
			}
		}
	}
	
	static Pail<Login> createUncompressedPail(FileSystem fs) throws IOException {
		return Pail.create(fs,
				"/tmp/logins",
				new PartitionedLoginPailStructure());
		
	}
	
	static Pail<Login> createCompressedPail(FileSystem fs) throws IOException {
		Map<String, Object> options = new HashMap<String, Object>();
		options.put(SequenceFileFormat.CODEC_ARG,
				SequenceFileFormat.CODEC_ARG_GZIP);
		options.put(SequenceFileFormat.TYPE_ARG,
				SequenceFileFormat.TYPE_ARG_BLOCK);
		LoginPailStructure struct = new LoginPailStructure();
		return Pail.create(fs, "/tmp/logins",
				new PailSpec("SequenceFile", options, struct));
		
	}
	public static void writeLogins() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<Login> loginPail = compress ? createCompressedPail(fs) : createUncompressedPail(fs);
		TypedRecordOutputStream out = loginPail.openWrite();
		out.writeObject(new Login("alex", 1352679231));
		out.writeObject(new Login("bob", 1352674216));
		out.writeObject(new Login("charlie", 1267421658));
		out.close();
	}
	public static void readLogins(int year, int month, int day) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopPrefix + "/conf/core-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		Pail<Login> loginPail = new Pail<Login>(fs, "/tmp/logins");
		for(Login l : loginPail) {
			Date objectDate = new Date(l.loginUnixTime * 1000L);
			int objYear = objectDate.getYear() + 1900;
			int objMonth = objectDate.getMonth() + 1;
			int objDay = objectDate.getDate();
			
			if((objYear == year) & (objMonth == month) & (objDay == day)){
				System.out.println(l.userName + " " + l.loginUnixTime);
			}
		}
	}
	static String hadoopPrefix = System.getenv("HADOOP_PREFIX");
	static boolean compress = false;
	public static void main(String[] args) {
		if(hadoopPrefix == null) {
			throw new RuntimeException("Please set HADOOP_PREFIX environment variable");
		}
		try {
			if(args[0].equals("s")) {
				writeLogins();
			} if (args[0].equals("c")) {
				compress = true;
				writeLogins();
			} else {
				int year = Integer.parseInt(args[1]);
				int month = Integer.parseInt(args[2]);
				int day = Integer.parseInt(args[3]);
				
				Date date = new Date(year, month, day);
				readLogins(date.getYear(), date.getMonth(), date.getDate());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
