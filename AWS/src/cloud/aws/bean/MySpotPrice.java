package cloud.aws.bean;

import com.amazonaws.services.ec2.model.SpotPrice;

public class MySpotPrice extends SpotPrice implements Comparable<MySpotPrice> {

	private static final long serialVersionUID = -8846721871634915445L;
	
	public MySpotPrice() {
	}

	public MySpotPrice(SpotPrice s, Boolean x) {
		setInstanceType(s.getInstanceType());
		setProductDescription(s.getProductDescription());
		setSpotPrice(s.getSpotPrice());
		setTimestamp(s.getTimestamp());
		setAvailabilityZone(s.getAvailabilityZone());
	}
	
	public Float getSpotPriceFloat() {
		return Float.valueOf(getSpotPrice());
	}
	
	
	@Override
	public int compareTo(MySpotPrice o) {
		return getTimestamp().compareTo(o.getTimestamp());
	}

	@Override
	public String toString() {
		return "[Zone="+ getAvailabilityZone() +", instance="+ getInstanceType() +","
				+" price="+  String.format("%f", getSpotPriceFloat()) +", timestamp="+ getTimestamp() +"]";
	}
	
}
