package cloud.aws.task;

import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.SdkBaseException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AvailabilityZone;

import cloud.aws.core.AWSManager;
import core.exceptions.ParameterRequiredException;
import core.exceptions.TooManyConnectionsException;
import core.util.PropertiesUtil;

public class SpotHistoryManager implements Runnable {
	
	private static final int SLEEP_BETWEEEN_ZONES = 2000;
	private static final String DEFAULT_REGION = "amazon.default.region";
	private static final String REGIONS_KEY = "amazon.regions";

	private final Logger log = LogManager.getLogger(SpotHistoryManager.class);
	
	private AmazonEC2 ec2;
	
	public SpotHistoryManager() throws ParameterRequiredException {
		
		String defaultRegion = PropertiesUtil.getInstance().getProperty(DEFAULT_REGION);
		
		Regions region = null;
		try {
			region = Regions.fromName(defaultRegion);
			this.ec2 = AWSManager.getInstance().getAmazonEC2(region);
		} catch (IllegalArgumentException e) {
			log.error("Error when trying to get "+ defaultRegion +": "+ e.getMessage());
			System.exit(-1);
		}
		
	}
	
	public AmazonEC2 getEc2() {
		return this.ec2;
	}
	
	@Override
	public void run() {
		
		Long mainTimeInit = Calendar.getInstance().getTimeInMillis();

		log.info("Starting SpotHistoryManager.");
		List<String> regionList = PropertiesUtil.getInstance().getList(REGIONS_KEY);
		log.info("Regions to be processed: "+ regionList);
		
		for (String reg : regionList) {
			
			log.info("########## Starting Region: "+ reg);
			
			Regions region = Regions.fromName(reg);
			
			if (Region.getRegion(region).isServiceSupported(AmazonEC2.ENDPOINT_PREFIX)) {
				
				try {
					AmazonEC2 ec2Exec = AWSManager.getInstance().getAmazonEC2(region);
					
					List<AvailabilityZone> availabilityZones = ec2Exec.describeAvailabilityZones().getAvailabilityZones();
					
					log.info("Zones to be processed in "+ region +": "+ availabilityZones.stream().map(AvailabilityZone::getZoneName).collect(Collectors.toList()));
					
					for (AvailabilityZone zone : availabilityZones) {
						
						log.info("##### Starting Zone: "+ zone.getZoneName());
						Long zoneTimeInit = Calendar.getInstance().getTimeInMillis();
						
						try {
							new SpotHistoryZoneManager(ec2Exec, region, zone).process();
						} catch (TooManyConnectionsException e) {
							log.error("Failed to process zone "+ zone.getZoneName() +": "+ e.getMessage());
						}
						
						Long zoneTimeEnd = Calendar.getInstance().getTimeInMillis();
						log.info("##### Finished Zone: "+ zone.getZoneName() +" in "+ LocalTime.MIN.plus(zoneTimeEnd-zoneTimeInit, ChronoUnit.MILLIS).toString());
						
						Thread.sleep(SLEEP_BETWEEEN_ZONES);
					}
					
				} catch (SdkBaseException e) {
					log.error("Error when try to call ec2 in "+ region.getName() +": "+ e.getMessage());
				} catch (InterruptedException e) {
					log.error("Error: "+ e.getMessage());
				}
				
			}
			
			log.info("########## Ending Region..: "+ reg);
			
		}
		
		Long mainTimeEnd = Calendar.getInstance().getTimeInMillis();
        log.info("Finished all zones in "+ LocalTime.MIN.plus(mainTimeEnd-mainTimeInit, ChronoUnit.MILLIS).toString());
	}

}