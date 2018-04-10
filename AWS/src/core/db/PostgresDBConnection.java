package core.db;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import core.exceptions.TooManyConnectionsException;
import core.util.GlobalCount;
import core.util.PropertiesUtil;

public class PostgresDBConnection {
	
	private static Map<Integer, Connection> pool = new HashMap<>();
	
	private static final Logger log = LogManager.getLogger(PostgresDBConnection.class);
	
	private static final int MAX_CONNECTIONS = 100;
	
	private static PostgresDBConnection uniqueInstance = null;
	private Connection connection;
	
	
	private PostgresDBConnection() {
//		this.connection = createConnection();
	}
	
	private Connection createConnection() {
		
		if (pool.size() > MAX_CONNECTIONS) {
			throw new TooManyConnectionsException(pool.size(), MAX_CONNECTIONS);
		}
		
		Connection newConnection = null;
		
		PropertiesUtil prop = PropertiesUtil.getInstance();
		String host = prop.getProperty("pg.host");
		String port = prop.getProperty("pg.port");
		String db = prop.getProperty("pg.database");
		String user = prop.getProperty("pg.user");
		String pass = prop.getProperty("pg.password");
		
		String databaseUrl = "jdbc:postgresql://"+ host +":"+ port +"/"+ db;
		String simpleDatabaseUrl = host +":"+ port +"/"+ db;
		
		try {
			log.info("Trying to connect to database "+ simpleDatabaseUrl);
			
			Class.forName("org.postgresql.Driver").newInstance();
			newConnection = DriverManager.getConnection(databaseUrl, user, pass);
			newConnection.setAutoCommit(false);
		} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			log.error("Error to connect database: "+ e.getMessage());
			System.exit(-1);
		}
		
		if (newConnection != null) {
			pool.put(newConnection.hashCode(), newConnection);
			log.info("Connected to "+ simpleDatabaseUrl +". "+ pool.size() +" connections.");
		}
		
		return newConnection;
	}
	
	public static void closeConnection(Connection con) throws SQLException {
		if (con != null) {
			con.close();
			pool.remove(con.hashCode());
		}
	}
	
	public static PostgresDBConnection getInstance() {
		if (uniqueInstance == null) {
			uniqueInstance = new PostgresDBConnection();
		}
		return uniqueInstance;
	}
	
	public Connection getConnection() {
		return connection;
	}
	
	public Connection getConnectionFromPool() throws TooManyConnectionsException {
		return createConnection();
	}
	
	public void copyFromCSV(String table, File[] files) throws Exception {
		
		if (StringUtils.isEmpty(table) || (files != null && files.length == 0)) {
			throw new IllegalArgumentException("Table and files are required to import CSV files to PostgreSQL! ");
		}
		
		Connection con = getConnectionFromPool();
		
		Statement stmt = con.createStatement();
		stmt.executeUpdate("TRUNCATE "+ table);
		stmt.close();
		con.commit();
		
		try {
			closeConnection(con);
		} catch (SQLException e) {
			log.error("Error when disconnect database in in TRUNCATE command: "+ e.getMessage());
		}

		
		int poolSize = Runtime.getRuntime().availableProcessors() * 2;
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		
		String countKey = "importCasesToPostgresThreadCount";
		
		for (File csvFile : files) {
			
			Runnable r = new Runnable() {
				@Override
				public void run() {
					Connection newConnection = getConnectionFromPool();
					try {
						CopyManager copyManager = new CopyManager((BaseConnection) newConnection);
						
						log.info("Importing file "+ csvFile.getAbsolutePath());
						FileReader fileReader = new FileReader(csvFile);
						String copyCommand = "COPY "+ table +" FROM STDIN WITH DELIMITER ';' CSV HEADER;";
						long rows = copyManager.copyIn(copyCommand, fileReader);
						log.info("Rows in "+ csvFile.getName() +": "+ rows);
						newConnection.commit();
						GlobalCount.reduceCount(countKey);
					} catch (Exception e) {
						log.error("Error when importing "+ csvFile +" to postgres: "+ e.getMessage());
					} finally {
						try {
							closeConnection(newConnection);
						} catch (SQLException e) {
							log.error("Error when disconnect database in "+ csvFile +" to postgres: "+ e.getMessage());
						}
					}
				}
			};
			executor.execute(r);
			GlobalCount.addCount(countKey);
			
		}
		
		executor.shutdown();
        while (!executor.isTerminated()) {
        	Thread.sleep(5000);
			log.info("Waiting...");
        }
        log.info("Done importing "+ files.length +" files.");
	}
	
	public static void main(String[] args) throws Exception {
		PostgresDBConnection.getInstance().copyFromCSV("cases", Paths.get("/tmp/exec/all_instances_all_regions").toFile().listFiles());
	}
	

}