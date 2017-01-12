package lasige.steeldb.Replica;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

public class InitReplica {

	public static void main(String[] args) {
		System.out.println("CHARSET: " + Charset.defaultCharset().toString() + ", ENCODING: " + System.getProperty("file.encoding"));
		if (args.length < 1) {
			usage();
		}
		Properties config = new Properties();
		try {
			config.load(new FileInputStream(args[0]));
			System.out.println("Configuration file loaded from: " + args[0]);
		} catch (IOException e) {
			System.out.println("Error: Unable to open configuration file");
			System.exit(-1);
		}
		
		String driver = config.getProperty("driver");
		
		try {
			Class.forName(driver).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int id = Integer.parseInt(config.getProperty("replica.id"));
		
                int master = (args.length > 1 ? Integer.parseInt(args[1]) : 0);
                
		new Replica(id, master, config);
	}

	public static void usage() {
		System.out.println("Use: java lasige.steeldb.Server <configfilename>.properties [master id]");
		System.exit(-1);
	}
}

