package main;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cloud.aws.task.SpotHistoryManager;
import cloud.aws.util.AWSUtil;

public class SpotHistoryMain {
	
	private static Logger log = null;
	
	static {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		log = LogManager.getLogger(SpotHistoryMain.class);
		log.debug("Properties configured");
	}
	
	public static void main(String[] args) {
		
		log.info("Initializing process in "+ AWSUtil.startedTime.getTime());
		
		try {
			new Thread(new SpotHistoryManager(), "ThreadSpotHistoryManager").start();
		} catch (Exception e) {
			System.out.println("Error: "+ e.getMessage());
		}
		
	}

}
