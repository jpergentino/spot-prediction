package core.executor.cbr;

import de.dfki.mycbr.core.casebase.Instance;

public class Case {

	private String region;
	private String zone;
	private String instance;
	private int dayOfWeek;
	private int hourOfDay;
	private int timeToRevocation; // in minutes
	private double additionAllowed; // in percent
	private double initValue;
	private double endValue;
	private long initTime;
	private long endTime;
	private boolean censored = false; 
	
	private int skipRecords;

	public Case() {
	}
	
	public Case(int timeToRevocation, boolean censored) {
		super();
		this.timeToRevocation = timeToRevocation;
		this.censored = censored;
	}

	public Case(String instance, int dayOfWeek, int hourOfDay, int additionAllowed, int executionTime) {
		this.instance = instance;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
		this.additionAllowed = additionAllowed;
		this.timeToRevocation = executionTime;
	}

	public Case(String instance, int dayOfWeek, int hourOfDay, int additionAllowed, int executionTime, double actualValue, long initTime, long endTime) {
		this.instance = instance;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
		this.additionAllowed = additionAllowed;
		this.timeToRevocation = executionTime;
		this.initValue = actualValue;
		this.initTime = initTime;
		this.endTime = endTime;
	}
	
	public Case(String region, String zone, String instance, int dayOfWeek, int hourOfDay, int additionAllowed, int executionTime, double actualValue, long initTime, long endTime) {
		this.region = region;
		this.zone = zone;
		this.instance = instance;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
		this.additionAllowed = additionAllowed;
		this.timeToRevocation = executionTime;
		this.initValue = actualValue;
		this.initTime = initTime;
		this.endTime = endTime;
	}
	
	public Case(String region, String zone, String instance) {
		this.region = region;
		this.zone = zone;
		this.instance = instance;
	}
	
	public static String toStringHead() {
		return "region;zone;instance;dayOfWeek;hourOfDay;additionAllowed;censored;initTime;endTime;timeToRevocation;skipRecords;initValue;endValue";
	}

	public String toStringCase() {
		return 	region +";"+
				zone +";"+
				instance +";"+ 
				String.format("%02d", dayOfWeek) +";"+ 
				String.format("%02d", hourOfDay) +";"+ 
				additionAllowed +";"+ 
				(censored ? "1" : "0") +";"+ 
				initTime +";"+ 
				endTime +";"+ 
				String.format("%06d", timeToRevocation) +";"+
				String.format("%05d", skipRecords) +";"+
				initValue +";"+
				endValue;
	}

	public Case fromInstance(Instance instanceCbr) {
		instanceCbr.getAttributes().entrySet().stream().forEach(x -> {

			switch (x.getKey().getName()) {

			case CBREngine.ATTR_NAME_INSTANCE:
				instance = x.getValue().getValueAsString();
				break;

			case CBREngine.ATTR_NAME_EXECUTION_DAY:
				dayOfWeek = Integer.valueOf(x.getValue().getValueAsString());
				break;

			case CBREngine.ATTR_NAME_EXECUTION_HOUR:
				hourOfDay = Integer.valueOf(x.getValue().getValueAsString());
				break;

			case CBREngine.ATTR_NAME_EXECUTION_TIME:
				timeToRevocation = Integer.valueOf(x.getValue().getValueAsString());
				break;

			default:
				break;
			}

		});

		return this;
	}

	public String getInstance() {
		return instance;
	}

	public void setInstance(String instance) {
		this.instance = instance;
	}

	public int getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(int dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public int getHourOfDay() {
		return hourOfDay;
	}

	public void setHourOfDay(int hourOfDay) {
		this.hourOfDay = hourOfDay;
	}

	public int getTimeToRevocation() {
		return timeToRevocation;
	}

	public void setTimeToRevocation(Integer executionTime) {
		this.timeToRevocation = executionTime;
	}

	@Override
	public String toString() {
		return "Case [instance=" + instance + ", dayOfWeek=" + dayOfWeek + ", hourOfDay=" + hourOfDay
				+ ", timeToRevocation=" + timeToRevocation + ", additionAllowed=" + additionAllowed + ", actualValue="
				+ initValue + ", actualTime=" + endTime + ", censored=" + censored + ", skipRecords=" + skipRecords
				+ "]";
	}

	public Integer getSkipRecords() {
		return skipRecords;
	}

	public void setSkipRecords(Integer skipRecords) {
		this.skipRecords = skipRecords;
	}

	public double getAdditionAllowed() {
		return additionAllowed;
	}

	public void setAdditionAllowed(double additionAllowed) {
		this.additionAllowed = additionAllowed;
	}

	public double getInitValue() {
		return initValue;
	}

	public void setInitValue(double actualValue) {
		this.initValue = actualValue;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long actualTime) {
		this.endTime = actualTime;
	}

	public void setSkipRecords(int skipRecords) {
		this.skipRecords = skipRecords;
	}

	public boolean isCensored() {
		return censored;
	}

	public void setCensored(boolean censored) {
		this.censored = censored;
	}

	public void setTimeToRevocation(int timeToRevocation) {
		this.timeToRevocation = timeToRevocation;
	}

	public long getInitTime() {
		return initTime;
	}

	public void setInitTime(long initTime) {
		this.initTime = initTime;
	}

	public double getEndValue() {
		return endValue;
	}

	public void setEndValue(double endValue) {
		this.endValue = endValue;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getZone() {
		return zone;
	}

	public void setZone(String zone) {
		this.zone = zone;
	}

}