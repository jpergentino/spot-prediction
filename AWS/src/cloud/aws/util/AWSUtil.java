package cloud.aws.util;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.InstanceType;

public final class AWSUtil {
	

	public static final Calendar startedTime;
	
	public static final String[] usedRegions = new String[]{ 
//			Regions.US_WEST_1.getName(), 
			Regions.US_WEST_2.getName(), 
	};
	public static final String[] usedZones = new String[]{ 
//			Regions.US_WEST_1.getName() +"b", 
//			Regions.US_WEST_1.getName() +"c", 
			Regions.US_WEST_2.getName() +"a", 
//			Regions.US_WEST_2.getName() +"b", 
//			Regions.US_WEST_2.getName() +"c", 
	};
	
//	public static final List<String> usedInstances = Arrays.asList(InstanceType.values()).stream().map(x -> x.toString()).sorted().collect(Collectors.toList());
	public static final List<String> usedInstances = Arrays.asList(
			
			// General purpose
//			InstanceType.C1Medium.toString()	// 1	3.75	Moderate
			InstanceType.M1Large.toString()	// 1	3.75	Moderate
//			InstanceType.M3Large.toString(), 	// 2	7.5		Moderate
//			InstanceType.M4Large.toString(), 	// 2	8		High
////			InstanceType.M4Xlarge.toString(), 	// 4	16		High
//			
//			// Compute Optimized
//			InstanceType.C4Large.toString(),	// 2	1.7		Moderate
////			InstanceType.C4Xlarge.toString(),	// 4	7.50	Moderate
//			
//			// Memory Optimized
////			InstanceType.R3Large.toString(),	// 2 	15		Moderate
////			InstanceType.R3Xlarge.toString(),	// 4 	30,5	Moderate
//			InstanceType.R4Large.toString(),	// 2 	15.25	10 Gbit
//			InstanceType.R4Xlarge.toString(),	// 4 	30,5	10 Gbit
			
			// GPU
//			InstanceType.P2Xlarge.toString()	// 4	61		GPU: 	1	12
	);
	
	
	static {
		startedTime = Calendar.getInstance();
	}
	
	public static String fullInstanceName(String reg, String zone, String instance) {
		return reg +"/"+ zone +"/"+ instance;
	}
	
	public static String fullInstanceName(Regions reg, AvailabilityZone zone, InstanceType instance) {
		return fullInstanceName(reg, zone.getZoneName(), instance.toString());
	}

	public static String fullInstanceName(Regions reg, String zone, String instance) {
		return reg.getName() +"/"+ zone +"/"+ instance;
	}
	
}
