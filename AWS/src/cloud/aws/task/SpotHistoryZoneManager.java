package cloud.aws.task;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.SpotPrice;

import cloud.aws.dao.SpotDAO;
import cloud.aws.task.thread.ThreadSpotPriceInstance;
import cloud.aws.util.TimerUtil;
import core.exceptions.TooManyConnectionsException;
import core.util.PropertiesUtil;

public class SpotHistoryZoneManager {
	
	private static final String SPOTHISTORY_THREAD_POOL_SIZE = "spothistory.thread.pool.size";

	private Logger log = LogManager.getLogger(SpotHistoryZoneManager.class);
	
	private Map<InstanceType, Boolean> completedMap = new HashMap<>();
	
	private AmazonEC2 ec2;
	
	private Regions region;
	private AvailabilityZone zone;
	private int attemptCount = 0;

	public SpotHistoryZoneManager(AmazonEC2 ec2, Regions reg, AvailabilityZone zone) {
		this.ec2 = ec2;
		this.region = reg;
		this.zone = zone;
		for (InstanceType instance : InstanceType.values()) {
			completedMap.put(instance, false);
		}
	}
	
	public void addCompletedInstance(InstanceType instance, List<SpotPrice> spotPriceList, boolean hasHistory) throws TooManyConnectionsException {
		
		boolean ok = true;
		
		if (spotPriceList != null && !spotPriceList.isEmpty()) {
			try {
				
				if (hasHistory) {
					new SpotDAO().saveSpotPriceBatch(spotPriceList, region);
				} else {
					new SpotDAO().saveSpotPrice(spotPriceList, region);
				}
				
			} catch (SQLException e) {
				ok = false;
				log.error("Error when trying to save into db: "+ e.getMessage());
			}
		}

		if (ok) {
			completedMap.put(instance, true);
		}
		
	}

	public void process() throws TooManyConnectionsException {
		
		if (++attemptCount > 1) {
			log.info("Attempting again in:");
			for (InstanceType it : completedMap.keySet()) {
				if (completedMap.get(it) == false) {
					log.info(it.toString());
				}
				
			}
		}
		
		String fullZoneName = region +"/"+ zone.getZoneName();
		
		if (ec2 == null) {
			log.debug("Need an AmazonEC2 instance to continue in "+ fullZoneName);
			return;
		}
		
		int threadPoolSize = PropertiesUtil.getInstance().getInteger(SPOTHISTORY_THREAD_POOL_SIZE);
		log.debug("Thread Pool Size: "+ threadPoolSize);
		
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		
		for (InstanceType instance : completedMap.keySet()) {
			if (completedMap.get(instance) == false) {
				executor.execute(new ThreadSpotPriceInstance(this, ec2, region, zone, instance));
			}
		}
		
		executor.shutdown();
		while (!executor.isTerminated()) {
			TimerUtil.sleep(5);
		}
        
        // process again if some instances was not successfully completed
		if (completedMap.containsValue(false)) {
			process();
		}
		
	}
	
}