package core.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import core.exceptions.TooManyConnectionsException;
import core.util.PropertiesUtil;

public class MySQLDBConnection {
	
	private static final Map<Integer, Connection> pool = new HashMap<>();
	
	private static final Logger log = LogManager.getLogger(MySQLDBConnection.class);
	
	private static final int MAX_CONNECTIONS = 100;
	
	private static MySQLDBConnection uniqueInstance = null;
	private Connection connection;
	
	
	private MySQLDBConnection() {
		this.connection = createConnection();
	}
	
	private Connection createConnection() {
		
		if (pool.size() > MAX_CONNECTIONS) {
			throw new TooManyConnectionsException(pool.size(), MAX_CONNECTIONS);
		}
		
		Connection newConnection = null;
		
		PropertiesUtil prop = PropertiesUtil.getInstance();
		String host = prop.getProperty("mysql.host");
		String port = prop.getProperty("mysql.port");
		String db = prop.getProperty("mysql.database");
		String user = prop.getProperty("mysql.user");
		String pass = prop.getProperty("mysql.password");
		
		String databaseUrl = "jdbc:mysql://"+ host +":"+ port +"/"+ db +"?autoReconnect=true&useSSL=false";
		String simpleDatabaseUrl = host +":"+ port +"/"+ db;
		
		try {
			log.info("Trying to connect to database "+ simpleDatabaseUrl);
			
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			newConnection = DriverManager.getConnection(databaseUrl, user, pass);
			newConnection.setAutoCommit(false);
		} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			log.error("Error to connect database: "+ e.getMessage());
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
	
	public static MySQLDBConnection getInstance() {
		if (uniqueInstance == null) {
			uniqueInstance = new MySQLDBConnection();
		}
		return uniqueInstance;
	}
	
	public Connection getConnection() {
		return connection;
	}
	
	public Connection getConnectionFromPool() throws TooManyConnectionsException {
		return createConnection();
	}
	

}