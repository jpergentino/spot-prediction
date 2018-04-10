package cloud.aws.task.thread;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryRequest;
import com.amazonaws.services.ec2.model.DescribeSpotPriceHistoryResult;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.SpotPrice;

import cloud.aws.dao.SpotDAO;
import cloud.aws.task.SpotHistoryZoneManager;
import cloud.aws.util.AWSUtil;
import core.util.PropertiesUtil;

public class ThreadSpotPriceInstance implements Runnable {
	
	private static final String PROP_PRODUCT_DESCRIPTION = "instance.productDescriptions";

	private Logger log = LogManager.getLogger(ThreadSpotPriceInstance.class);
	
	private AmazonEC2 ec2;
	
	private Regions region;
	private AvailabilityZone zone;
	private InstanceType instance;
	private SpotHistoryZoneManager myManager;
	
	private SpotDAO spotDAO;


	public ThreadSpotPriceInstance(SpotHistoryZoneManager spotZoneHistoryManager, AmazonEC2 ec2, Regions reg, AvailabilityZone zone, InstanceType instance) {
		this.myManager = spotZoneHistoryManager;
		this.ec2 = ec2;
		this.region = reg;
		this.zone = zone;
		this.instance = instance;
	}

	@Override
	public void run() {
		this.spotDAO = new SpotDAO();
		
		String fullInstanceName = AWSUtil.fullInstanceName(region, zone, instance);
		
		log.info("Trying to get spot history of "+ fullInstanceName);
		
		if (ec2 == null) {
			log.debug("Need an AmazonEC2 instance to continue in "+ fullInstanceName);
			return;
		}
		
		List<SpotPrice> spotPriceBuffer = new LinkedList<>();
		
		Long lastRecord = null;
		try {
			lastRecord = spotDAO.getLastRecord(region, zone, instance);
		} catch (SQLException eSql) {
			log.error("Error when trying to get last record of "+ fullInstanceName +": "+ eSql.getMessage());
		}
		
		try {
			
			// creating a filter to get spot history only from zone, instance and product description
			DescribeSpotPriceHistoryRequest req = new DescribeSpotPriceHistoryRequest().withInstanceTypes(instance);
			req.setAvailabilityZone(zone.getZoneName());
			
			if (lastRecord != null) {
				Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				c.setTimeInMillis(lastRecord);
				log.debug(fullInstanceName +" has a last record: "+ c.getTime());
				c.add(Calendar.MINUTE, 1);
				req.setStartTime(c.getTime());
			}
			
			List<String> productDescriptions = PropertiesUtil.getInstance().getList(PROP_PRODUCT_DESCRIPTION);
			if (productDescriptions != null && !productDescriptions.isEmpty()) {
				req.setProductDescriptions(productDescriptions);
			}
			
			String nextToken = "";
			
			do {
				
				req.setNextToken(nextToken);
				
				DescribeSpotPriceHistoryResult result = ec2.describeSpotPriceHistory(req);
				
				if (result.getSpotPriceHistory().isEmpty()) {
					log.info("Nothing on "+ fullInstanceName);
				} else {
					spotPriceBuffer.addAll(result.getSpotPriceHistory());
				}
				
				nextToken = result.getNextToken();
				
			} while(!nextToken.isEmpty());
			
			boolean hasHistory = lastRecord == null; 
			
			myManager.addCompletedInstance(instance, spotPriceBuffer, hasHistory);
			
			
		} catch (Exception e) {
			log.error("Error when trying to get spot history of "+ fullInstanceName +": "+ e.getMessage());
		}

		
		log.info("Finished "+ fullInstanceName);
		spotDAO.closeConnection();
	}
	
}