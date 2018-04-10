package core.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import core.exceptions.ParameterRequiredException;

public class PropertiesUtil {
	
	public static final String CONFIG_PROPERTIES = "config.properties";
	
	private final Logger log = LogManager.getLogger(PropertiesUtil.class);
	
	private static PropertiesUtil instance = null;
	
	private Properties properties;
	
	public static PropertiesUtil getInstance() {
		if (instance == null) {
			instance = new PropertiesUtil();
		}
		return instance;
	}

	private PropertiesUtil() {
		properties = new Properties();
		loadFile();
		
		List<String> requiredParams = new ArrayList<String>();
		
		requiredParams.add("amazon.regions");
		requiredParams.add("amazon.default.region");
		requiredParams.add("amazon.key.access");
		requiredParams.add("amazon.key.secret");
		requiredParams.add("instance.productDescriptions");
		requiredParams.add("mysql.host");
		requiredParams.add("mysql.port");
		requiredParams.add("mysql.database");
		requiredParams.add("mysql.user");
		requiredParams.add("mysql.password");
		requiredParams.add("spothistory.thread.pool.size");
		
		validateRequiredParams(requiredParams);
	}
	
	public String getAccessKey() {
		return properties.getProperty("amazon.key.access");
	}

	public String getSecretKey() {
		return properties.getProperty("amazon.key.secret");
	}
	
	public String getDefaultRegion() {
		return properties.getProperty("amazon.default.region");
	}
	
	private void validateRequiredParams(List<String> requiredParams) {
		for (String key : requiredParams) {
			if (!properties.containsKey(key)) {
				log.error("Parameter required: "+ key);
				log.error("Exiting...");
				System.exit(-1);
			}
		}
	}

	private void loadFile() {
		InputStream input = null;

		try {
			input = new FileInputStream(CONFIG_PROPERTIES);
			properties.load(input);
		} catch (Exception ex) {
			log.error("Error when trying to load "+ CONFIG_PROPERTIES +" file: "+ ex.getMessage());
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error("Error when trying to close "+ CONFIG_PROPERTIES +" file: "+ e.getMessage());
				}
			}
		}
	}
	
	public String getProperty(String key) {
		loadFile();
		return properties.getProperty(key);
	}

	public Integer getInteger(String key) {
		return Integer.parseInt(getProperty(key));
	}
	
	public Boolean getBoolean(String key) {
		return Boolean.valueOf(getProperty(key));
	}
	
	public String[] getArray(String key) {
		List<String> tempList = new LinkedList<String>();
		
		for (String string : getProperty(key).split(",")) {
			if (!StringUtils.isBlank(string)) {
				tempList.add(StringUtils.trim(string));
			}
		}
		
		return tempList.toArray(new String[tempList.size()]);
	}
	
	public List<String> getList(String key) {
		List<String> returnList = new LinkedList<String>();
		
		for (String string : getProperty(key).split(",")) {
			if (!StringUtils.isBlank(string)) {
				returnList.add(StringUtils.trim(string));
			}
		}
		
		return returnList;
	}
	
	public void printProperties() {
		
		Enumeration<?> e = properties.propertyNames();
		while (e.hasMoreElements()) {
			String key = (String) e.nextElement();
			String value = properties.getProperty(key);
			System.out.println("Key : " + key + ", Value : " + value);
		}

	}

	public void save() {
		
		OutputStream output = null;

		try {

			log.info("Saving "+ CONFIG_PROPERTIES +" file.");
			output = new FileOutputStream(CONFIG_PROPERTIES);
			properties.store(output, null);

		} catch (IOException ex) {
			log.error("Error when trying to save "+ CONFIG_PROPERTIES +" file: "+ ex.getMessage());
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					log.error("Error when trying to close "+ CONFIG_PROPERTIES +" file: "+ e.getMessage());
				}
			}
		}
		
		
	}
	
	public static void main(String[] args) throws ParameterRequiredException {
		List<String> t = PropertiesUtil.getInstance().getList("amazon.regions");
		System.out.println("List size: "+ t.size());
		
		t.stream().forEach(System.out::println);
		
		
	}
	
}
