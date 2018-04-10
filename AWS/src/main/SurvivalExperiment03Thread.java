package main;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cloud.aws.bean.MySpotPrice;
import cloud.aws.dao.SpotDAO;
import cloud.aws.util.AWSUtil;
import core.executor.cbr.Case;
import core.util.DateUtil;
import core.util.KaplanMeierEstimator;
import core.util.KaplanMeierEstimator.Interval;
import core.util.Statistic;

public class SurvivalExperiment03Thread implements Runnable {
	
	private static final Logger log = LogManager.getLogger(SurvivalExperiment03Thread.class);

	private static final Date startedTime = new Date();
	
	private String region = null;
	private String zone = null;
	private String instance;
	
	// Confidence Interval defaults is 0.95%
	private double confidenceInterval = 0.95;
	private Integer minutesToTest;
	private Integer previousDaysToPriceReference;
	private Integer dayOfWeek = null;
	private Integer hourOfDay = null;
	private boolean minutesDefined = false;
	
	private SpotDAO dao;
	
	static {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		log.debug("Properties configured");
	}
	
	public SurvivalExperiment03Thread(String region, String zone, String instance, double confidenceInterval) {
		this.region = region;
		this.zone = zone;
		this.instance = instance;
		this.confidenceInterval = confidenceInterval;
	}
	
	public SurvivalExperiment03Thread(String region, String zone, String instance, double confidenceInterval, Integer minutesToTest, Integer previousDaysToPriceReference, Integer dayOfWeek, Integer hourOfDay) {
		this(region, zone, instance, confidenceInterval);
		this.minutesToTest = minutesToTest;
		this.previousDaysToPriceReference = previousDaysToPriceReference;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
		this.minutesDefined = this.minutesToTest != null;
	}
	
	
	public void process() throws SQLException {
		StringBuilder sb = new StringBuilder("Processing: "+ System.lineSeparator());
		sb.append("Instance..............: "+ instance + System.lineSeparator());
		sb.append("Conf. Interval........: "+ String.format(Locale.US, "%.2f", confidenceInterval) + System.lineSeparator());
		sb.append("Minutes Test..........: "+ minutesToTest +" Defined? " + minutesDefined + System.lineSeparator());
		sb.append("Previuos Days to Price: "+ String.format(Locale.US, "%2d", previousDaysToPriceReference) + System.lineSeparator());
		sb.append("Day of Week...........: "+ String.format(Locale.US, "%2d", dayOfWeek) + System.lineSeparator());
		sb.append("Hour of Day...........: "+ String.format(Locale.US, "%2d", hourOfDay) + System.lineSeparator());
		
		log.info(sb.toString());

		@SuppressWarnings("unchecked")
		List<MatrixRow>[][] matrixMedian = new ArrayList[8][24];
		@SuppressWarnings("unchecked")
		List<MatrixRow>[][] matrixMean = new ArrayList[8][24];
		
		// Initializing matrix
		for (int i = 1; i < 8; i++) {
			for (int j = 0; j < 24; j++) {
				matrixMedian[i][j] = new ArrayList<>();
				matrixMean[i][j] = new ArrayList<>();
			}
		}
		
		// Defining experiment date init
		Calendar baseTime = Calendar.getInstance();
		baseTime.set(Calendar.MONTH, 8); // 0=January, ..., 8=September
		baseTime.set(Calendar.DAY_OF_MONTH, 1); // day 1, 6th day of week
		baseTime.set(Calendar.HOUR_OF_DAY, 0); // 0 hour
		baseTime.set(Calendar.MINUTE, 0); // 0 minute
		baseTime.set(Calendar.SECOND, 0); // 0 second
		baseTime.set(Calendar.MILLISECOND, 0); // 0 second
		
		while (baseTime.get(Calendar.MONTH) <= 10) { // sep, oct, nov
			
//			log.info("Processing "+ instance +" on "+ baseTime.getTime());

			int localDayOfWeek = baseTime.get(Calendar.DAY_OF_WEEK);
			int localHourOfDay = baseTime.get(Calendar.HOUR_OF_DAY);
			
			MatrixRow matrixRow = null;
			if ((dayOfWeek == null && hourOfDay == null) || (dayOfWeek == localDayOfWeek && hourOfDay == localHourOfDay))  {
				try {
					matrixRow = simulateSurvivalProbability(localDayOfWeek, localHourOfDay, baseTime);
				} catch (Exception e) {
					log.error("Error when simulate survivor probability to "+ region +"/"+ zone +"/"+ instance +" at "+ localDayOfWeek +" and "+ localHourOfDay +": "+ e.getMessage());
				}
			}
			
			if (matrixRow != null) {
				matrixMedian[localDayOfWeek][localHourOfDay].add(matrixRow);
				matrixMean[localDayOfWeek][localHourOfDay].add(matrixRow);
			}
			
			baseTime.add(Calendar.HOUR_OF_DAY, 1);
			
		} // while baseTime

		printMatrix(false, "Median on "+ instance, matrixMedian);
		printMatrix(true, "Mean on "+ instance, matrixMean);

		plotMatrix(false, matrixMedian);
		plotMatrix(true, matrixMedian);
		
	}
	
	private MatrixRow simulateSurvivalProbability(Integer dayOfWeek, Integer hourOfDay, Calendar baseTime) throws SQLException {
		
		defineMinutesFromConfidenceInterval(dayOfWeek, hourOfDay);
		log("Interval in "+ (confidenceInterval * 100) +"%: "+ minutesToTest +" minutes until failure.");
		
		float basePrice = defineBasePrice(baseTime);
		log("Base price....: "+ String.format("%f", basePrice));
		
		List<MySpotPrice> changesAfterBaseTime = getPricesChangesAfterTime(baseTime);
		log("Price changes.: "+ changesAfterBaseTime.size());
		
		Double actualProbability = null;
		boolean died = false;
		for(MySpotPrice sp : changesAfterBaseTime) {
			
			if (sp.getSpotPriceFloat() > basePrice) {
				log("Started at.: "+ DateUtil.getFormattedDateTime(baseTime.getTime()));
				log("Died at....: "+ DateUtil.getFormattedDateTime(sp.getTimestamp()) +" with price $ "+ String.format("%f", sp.getSpotPriceFloat()));
				double durationInMinutes = TimeUnit.MILLISECONDS.toMinutes(sp.getTimestamp().getTime() - baseTime.getTimeInMillis());
				double probability = durationInMinutes / minutesToTest;
				log("Total alive: "+ durationInMinutes +"/"+ minutesToTest +" "+ String.format("%.2f", probability * 100)+ "%");
				died = true;
				actualProbability = probability > 1 ? 1.0 : probability;
				break;
			}
		}
		
		if (!died) {
			actualProbability = 1.0;
			log("Not died.");
		}
		
		return new MatrixRow(actualProbability, minutesToTest);
		
	}

	private List<MySpotPrice> getPricesChangesAfterTime(Calendar baseTime) throws SQLException {
		log("-------------------------------------------------------");
		log("GETTING EXPERIMENT 'UNKNOWN' DATA AFTER BASE TIME "+ DateUtil.getFormattedDateTime(baseTime.getTime()));
		log("-------------------------------------------------------");
		
		Calendar infiniteDate = Calendar.getInstance();
		infiniteDate.add(Calendar.YEAR, 10);
		
		List<MySpotPrice> changesAfterBaseTime = dao.findAll(region, zone, instance, baseTime.getTime(), infiniteDate.getTime());
		return changesAfterBaseTime;
	}

	private float defineBasePrice(Calendar baseTime) throws SQLException {
		log("-------------------------------------------------------");
		log("DEFINING BASE PRICE");
		log("-------------------------------------------------------");
		
		// getting a median of last N days
		Calendar previousDateTemp = Calendar.getInstance();
		previousDateTemp.setTimeInMillis(baseTime.getTimeInMillis());
		previousDateTemp.add(Calendar.DAY_OF_MONTH, previousDaysToPriceReference * -1);
		
		List<MySpotPrice> changes = dao.findAll(region, zone, instance, previousDateTemp.getTime(), baseTime.getTime());
		log("Price changes: "+ changes.size());
		List<Double> priceList = changes.stream().map(x -> Double.valueOf(x.getSpotPrice())).collect(Collectors.toList());
		
		double min = priceList.stream().reduce(Double.MAX_VALUE, Double::min);
		double max = priceList.stream().reduce(Double.MIN_VALUE, Double::max);
		
		double mean = Statistic.mean(priceList);
		double median = Statistic.median(priceList);
		double standardDeviation = Statistic.standardDeviation(priceList);
		log("Min price......: "+ String.format("%f", min));
		log("Max price......: "+ String.format("%f", max));
		log("Mean price.....: "+ String.format("%f", mean));
		log("Median price...: "+ String.format("%f", median));
		log("Standard Dev...: "+ String.format("%f", standardDeviation));
		
		return Double.valueOf(mean).floatValue();
	}

	private void defineMinutesFromConfidenceInterval(Integer dayOfWeek, Integer hourOfDay) throws SQLException {
		log("-------------------------------------------------------");
		log("GETTING SIMILAR CASES. DOW: "+ dayOfWeek +" and HOD: "+ hourOfDay);
		log("-------------------------------------------------------");
		
		// Getting cases with same REGION, ZONE, INSTANCE, DAY and HOUR
		List<Case> cases = dao.findCases(region, zone, instance, dayOfWeek, hourOfDay);
		
		// create a set of survivor time.
		List<Interval> intervals = KaplanMeierEstimator.compute(cases);
		log("Cases....: "+ cases.size());
		log("Intervals: "+ intervals.size());
		
		if (!minutesDefined) {
			// searching 'interval object' with confidence interval to find minutes until failure 
			Interval base = intervals.get(0);
			for (Interval n : intervals) {
//			 log(n.getEnd() +" - "+ String.format("%f", n.getCumulativeSurvival()) +" "+ n.getNumberCensured() +"/"+ n.getNumberDied());
				if (n.getCumulativeSurvival() >= confidenceInterval) {
					base = n;
				} else {
					break;
				}
			}
			minutesToTest = base.getEnd();
		}
		
	}
	
	
	
	private void plotMatrix(Boolean mean, List<MatrixRow>[][] matrixMedian) {
		List<Double> medians = new ArrayList<>();
		double min = Integer.MAX_VALUE;
		double max = Integer.MIN_VALUE;
		
		
		StringBuffer sb = new StringBuffer();
		sb.append("var 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23"+ System.lineSeparator());
		for (int i = 1; i <= 7; i++) {
			sb.append(i +" ");
			for (int j = 0; j < 24; j++) {
				if (matrixMedian[i][j] != null && !matrixMedian[i][j].isEmpty()) {
					List<Double> values = matrixMedian[i][j].stream().map(x -> x.value).collect(Collectors.toList());
					Double value = (mean ? Statistic.mean(values) : Statistic.median(values));
					medians.add(value);
					sb.append(String.format(Locale.US, "%.2f", value) +" ");
					min = value < min ? value : min;
					max = value > max ? value : max;
				} else {
					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}

		sb.append(System.lineSeparator());
		sb.append("#Times:"+ System.lineSeparator());
		sb.append("#     0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23"+ System.lineSeparator());
		//         1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234
		
		for (int i = 1; i <= 7; i++) {
			sb.append("#"+ i +" ");
			for (int j = 0; j < 24; j++) {
				List<MatrixRow> listOfValues = matrixMedian[i][j];
				if (listOfValues != null && !listOfValues.isEmpty()) {
					sb.append(String.format("%4d", listOfValues.get(0).minutes) +" ");
				} else {
					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}
		
		
		double totalMean = Statistic.mean(medians);
		double totalMedian = Statistic.median(medians);
		double totalStandardDeviation = Statistic.standardDeviation(medians);
		double totalCoefficientOfVariation = Statistic.coefficientOfVariation(medians);
		
		sb.append("# Min value.: "+ min + System.lineSeparator());
		sb.append("# Max value.: "+ max + System.lineSeparator());
		sb.append("# Conf. Int.: "+ String.format(Locale.US, "%.2f", confidenceInterval) + System.lineSeparator());
		sb.append("# Mean......: "+ String.format(Locale.US, "%.2f", totalMean) + System.lineSeparator());
		sb.append("# Median....: "+ String.format(Locale.US, "%.2f", totalMedian) + System.lineSeparator());
		sb.append("# Stand. Dev: "+ String.format(Locale.US, "%.2f", totalStandardDeviation) + System.lineSeparator());
		double toDown = totalMedian - totalStandardDeviation;
		double toUp = totalMedian + totalStandardDeviation;
		sb.append("# Range Medi: "+ String.format(Locale.US, "%.2f", toDown) +"-"+ String.format(Locale.US, "%.2f", totalStandardDeviation) +"-"+ String.format(Locale.US, "%.2f", toUp) + System.lineSeparator());
		
		
		StringBuilder filename = new StringBuilder();
		filename.append(instance);
		filename.append("-"+ (mean ? "mean" : "median"));
		filename.append("_"+ previousDaysToPriceReference);
		filename.append("_"+ String.format(Locale.US, "%.2f", confidenceInterval));
		filename.append("_"+ String.format(Locale.US, "%.2f", totalMean));
		filename.append("_"+ String.format(Locale.US, "%.2f", totalMedian));
		filename.append("_"+ String.format(Locale.US, "%.2f", totalStandardDeviation));
		filename.append("_"+ String.format(Locale.US, "%.2f", totalCoefficientOfVariation));
		filename.append(".txt");
		
		// only plot when instance exists on region/zone
		if (totalMedian > 0.0) {
			
			File parent = new File("plot", DateUtil.getFormattedDateTimeToFile(startedTime));
			if (!parent.exists()) {
				parent.mkdirs();
			}
			File file = new File(parent,  filename.toString());
			try {
				FileUtils.writeStringToFile(file, sb.toString(), Charset.defaultCharset());
			} catch (IOException e) {
				log.error("Error when trying to save file "+ file.toString() +": "+ e.getMessage());
			}
		}
		
	}
	
	

	private void printMatrix(Boolean mean, String title, List<MatrixRow>[][] matrixMedian) {
		List<Double> medians = new ArrayList<>();
		double min = Integer.MAX_VALUE;
		double max = Integer.MIN_VALUE;
		
		
		StringBuffer sb = new StringBuffer();
		sb.append(System.lineSeparator());
		sb.append("     0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23"+ System.lineSeparator());
		//         1 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00 1.00
		for (int i = 1; i <= 7; i++) {
			sb.append(i +" ");
			for (int j = 0; j < 24; j++) {
				if (matrixMedian[i][j] != null && !matrixMedian[i][j].isEmpty()) {
					List<Double> values = matrixMedian[i][j].stream().map(x -> x.value).collect(Collectors.toList());
					Double value = (mean ? Statistic.mean(values) : Statistic.median(values));
					medians.add(value);
					sb.append(String.format(Locale.US, "%.2f", value) +" ");
					min = value < min ? value : min;
					max = value > max ? value : max;
				} else {
					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}

		sb.append(System.lineSeparator());
		sb.append("Times:"+ System.lineSeparator());
		sb.append("     0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23"+ System.lineSeparator());
		//         1 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234 1234
		
		for (int i = 1; i <= 7; i++) {
			sb.append(i +" ");
			for (int j = 0; j < 24; j++) {
				List<MatrixRow> listOfValues = matrixMedian[i][j];
				if (listOfValues != null && !listOfValues.isEmpty()) {
					sb.append(String.format("%4d", listOfValues.get(0).minutes) +" ");
				} else {
					sb.append(String.format(Locale.US, "%.4f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}
		
		double totalMean = Statistic.mean(medians);
		double totalMedian = Statistic.median(medians);
		double totalStandardDeviation = Statistic.standardDeviation(medians);
		
		sb.append("Conf. Int.: "+ String.format(Locale.US, "%.2f", confidenceInterval) + System.lineSeparator());
		sb.append("Mean......: "+ String.format(Locale.US, "%.2f", totalMean) + System.lineSeparator());
		sb.append("Median....: "+ String.format(Locale.US, "%.2f", totalMedian) + System.lineSeparator());
		sb.append("Stand. Dev: "+ String.format(Locale.US, "%.2f", totalStandardDeviation) + System.lineSeparator());
		sb.append("Min value.: "+ min + System.lineSeparator());
		sb.append("Max value.: "+ max + System.lineSeparator());
		
		log.info(sb.toString());
	}
	
	@Override
	public void run() {
		try {
			this.dao = new SpotDAO();
			process();
		} catch (SQLException e) {
			log.error("Error when processing "+ instance +": "+ e.getMessage());
		} finally {
			dao.closeConnection();
		}
	}
	
	public void log(String text) {
//		log.info(text);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		
		int threadPoolSize = Runtime.getRuntime().availableProcessors() + 1;
		double[] confidenceInterval = new double[] {0.95, 0.98};
		int[] daysToPriceReference = new int[] {7};
		
		List<String> instancesToSimulate = AWSUtil.usedInstances;
		
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		int threadsAddedCount = 0;
		
		String[] regions = AWSUtil.usedRegions; 
		String[] zones = AWSUtil.usedZones;
		
		for (String region : regions) {
			for (String zone : zones) {
				for (String instance : instancesToSimulate) {
					for (Double confidence : confidenceInterval) {
						for (Integer daysBefore : daysToPriceReference) {
							executor.execute(new Thread(new SurvivalExperiment03Thread(region, zone, instance, confidence, null, daysBefore, null, null), "Thread_"+ instance +"_"+ confidence));
							threadsAddedCount++;
						}
					}
				}
			}
			
		}
		
		
		executor.shutdown();
        while (!executor.isTerminated()) {
        	Thread.sleep(30 * 1000);
        	if (executor instanceof ThreadPoolExecutor) {
        	    String queue = ((ThreadPoolExecutor) executor).getQueue().stream().map(r -> (Thread) r).map(Thread::getName).collect(Collectors.joining(","));
        	    long completed = ((ThreadPoolExecutor) executor).getCompletedTaskCount();
				log.debug("Pool status ("+ String.format("%02d", completed) +"/"+ String.format("%02d", threadsAddedCount) +"): Queue: "+ queue);
        	}
        }

		log.debug("Done.");
				
	}

	
}