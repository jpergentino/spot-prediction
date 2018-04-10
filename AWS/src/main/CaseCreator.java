package main;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AvailabilityZone;

import cloud.aws.AWSCBRCreator;
import cloud.aws.core.AWSManager;
import cloud.aws.dao.SpotDAO;
import cloud.aws.util.AWSUtil;
import core.db.PostgresDBConnection;
import core.util.DateUtil;

public class CaseCreator {
	
	private static final Logger log = LogManager.getLogger(CaseCreator.class);
	
	public static void main(String[] args) throws Exception {
		
		String CASES_FOLDER = System.getProperty("user.home") + File.separatorChar + "cases";
		
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		
		Boolean saveToDatabase = false;
		if (args.length == 0) {
			log.info("Please, specify a boolean parameter to define if will save into database. Assuming false.");
		} else {
			saveToDatabase = Boolean.valueOf(args[0]);
			log.info("Save to database? "+ saveToDatabase);
		}
		
		try {
			CASES_FOLDER = args[1];	
		} catch (ArrayIndexOutOfBoundsException e) {
			// do nothing
		}
		
		CASES_FOLDER = CASES_FOLDER + File.separatorChar +"aws_1";
		
		int folderCount = 0;
		boolean exists = Files.exists(Paths.get(CASES_FOLDER));
		
		while (exists) {
			String[] split = Paths.get(CASES_FOLDER).getFileName().toString().split("_");
			if (split.length == 2) {
				try {
					folderCount = Integer.parseInt(split[1]) + 1;
				} catch (Exception e) {
					// do nothing...
				}
			}
			
			CASES_FOLDER = Paths.get(CASES_FOLDER).getParent().toString() + File.separatorChar + "aws_"+ folderCount;
			exists = Files.exists(Paths.get(CASES_FOLDER));
		}
		
		Files.createDirectories(Paths.get(CASES_FOLDER));
		
		String[] regions = AWSUtil.usedRegions;
		List<String> instances = AWSUtil.usedInstances;
		
		log.info("Using "+ CASES_FOLDER +" as export folder.");
		log.info("Regions..:"+ Arrays.toString(regions));
		log.info("Instances:"+ instances);
		log.info("Initializing in 5 seconds...");
		Thread.sleep(5 * 1000);

		
		Date init = Calendar.getInstance().getTime();
		
		AmazonEC2 amazonEC2 = null;
		List<AvailabilityZone> availabilityZones = null;
		
		int poolSize = Runtime.getRuntime().availableProcessors() + 1;
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		log.info("Pool size: "+ poolSize);
		
		int threadsAddedCount = 0;
		
		for (String region : regions) {
			
			log.info("Starting to create cases to:");
			log.info("Region: "+ region);
			log.info("Instances: "+ instances);
			
			try {
				amazonEC2 = AWSManager.getInstance().getAmazonEC2(region);
				
				availabilityZones = amazonEC2.describeAvailabilityZones().getAvailabilityZones();
				
				for (AvailabilityZone z : availabilityZones) {
					
					for (String instance : instances) {
						
						File fileToSave = new File(CASES_FOLDER, "cases_"+ z.getZoneName() +"_"+ instance +".csv");
						
						if (fileToSave.exists()) {
							FileUtils.forceDelete(fileToSave);
						}
						
						Date d1 = DateUtil.getInitDate("2017/01/01");
						Date d2 = DateUtil.getEndOfDate("2017/08/31");
						
						AWSCBRCreator creator = new AWSCBRCreator(region, z.getZoneName(), instance, d1, d2, fileToSave, saveToDatabase);
						creator.setName(z.getZoneName() +"@"+ instance);
						
						executor.execute(creator);
						threadsAddedCount++;
					}
					
				}
				
			} catch (Exception e) {
				log.error("Error when trying to get "+ region +": "+ e.getMessage());
			} finally {
				if (availabilityZones != null) {
					availabilityZones.clear();
				}
				amazonEC2 = null;
			}
			
		}
		
		executor.shutdown();
        while (!executor.isTerminated()) {
        	Thread.sleep(5000);
        	
        	if (executor instanceof ThreadPoolExecutor) {
        	    String queue = ((ThreadPoolExecutor) executor).getQueue().stream().map(r -> (Thread) r).map(Thread::getName).collect(Collectors.joining(","));
        	    long completed = ((ThreadPoolExecutor) executor).getCompletedTaskCount();
				log.info("Pool status ("+ String.format("%02d", completed) +"/"+ String.format("%02d", threadsAddedCount) +"): Queue: "+ queue);
//        	    forEach(x -> System.out.println(((Thread)x).getName()));
        	}
        	
        }
        Date end = Calendar.getInstance().getTime();
        log.info("################## Done in "+ Minutes.minutesBetween(new DateTime(init.getTime()), new DateTime(end.getTime())).getMinutes() +" minutes.");
        
        log.info("Importing files to DB");
        PostgresDBConnection.getInstance().copyFromCSV(SpotDAO.TB_CASES, Paths.get(CASES_FOLDER).toFile().listFiles());
		
		
		
	}



}
