package main;

import java.io.File;
import java.io.IOException;
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

import com.amazonaws.regions.Regions;

import cloud.aws.bean.MySpotPrice;
import cloud.aws.dao.SpotDAO;
import cloud.aws.util.AWSUtil;
import core.executor.cbr.Case;
import core.util.DateUtil;
import core.util.KaplanMeierEstimator;
import core.util.KaplanMeierEstimator.Interval;
import core.util.Statistic;

public class SurvivalExperiment02Thread implements Runnable {
	
	private static final Logger log = LogManager.getLogger(SurvivalExperiment02Thread.class);

	private static final String REGION = Regions.US_WEST_1.getName();
	private static final String ZONE = Regions.US_WEST_1.getName() + "b";

	private static final Date startedTime = new Date();
	
	private String instance;
	
	// Confidence Interval defaults is 0.95%
	private double confidenceInterval = 0.95;
	private Integer minutesToTest;
	private Integer previousDaysToPriceReference;
	private Integer dayOfWeek = null;
	private Integer hourOfDay = null;
	
	private SpotDAO dao;


	
	static {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		log.debug("Properties configured");
	}
	
	public SurvivalExperiment02Thread(String instance, double confidenceInterval) {
		this.instance = instance;
		this.confidenceInterval = confidenceInterval;
	}
	
	public SurvivalExperiment02Thread(String instance, double confidenceInterval, Integer minutesToTest, Integer previousDaysToPriceReference, Integer dayOfWeek, Integer hourOfDay) {
		this(instance, confidenceInterval);
		this.minutesToTest = minutesToTest;
		this.previousDaysToPriceReference = previousDaysToPriceReference;
		this.dayOfWeek = dayOfWeek;
		this.hourOfDay = hourOfDay;
	}
	
	
	public void process() throws SQLException {

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
			
			MatrixRow survivalPercentualRow = null;
			if (dayOfWeek == null || hourOfDay == null) {
				survivalPercentualRow = processSurvivalPercentual(localDayOfWeek, localHourOfDay, baseTime);
			} else if (dayOfWeek == localDayOfWeek && hourOfDay == localHourOfDay){
				survivalPercentualRow = processSurvivalPercentual(localDayOfWeek, localHourOfDay, baseTime);
			}
			
			if (survivalPercentualRow != null) {
				matrixMedian[localDayOfWeek][localHourOfDay].add(survivalPercentualRow);
				matrixMean[localDayOfWeek][localHourOfDay].add(survivalPercentualRow);
			}
			
			baseTime.add(Calendar.HOUR_OF_DAY, 1);
			
		} // while baseTime

		printMatrix(false, "Median on "+ instance, matrixMedian);
		printMatrix(true, "Mean on "+ instance, matrixMean);

		plotMatrix(false, "median-"+ instance, matrixMedian);
		plotMatrix(true, "mean-"+ instance, matrixMedian);
		
	}
	
	private MatrixRow processSurvivalPercentual(int dayOfWeek, int hourOfDay, Calendar baseTime) throws SQLException {
		
		
		log("-------------------------------------------------------");
		log("GETTING SIMILAR CASES. DOW: "+ dayOfWeek +" and HOD: "+ hourOfDay);
		log("-------------------------------------------------------");
		
		// Getting cases with same REGION, ZONE, INSTANCE, DAY and HOUR
		List<Case> cases = dao.findCases(REGION, ZONE, instance, dayOfWeek, hourOfDay);
		
		// create a set of survivor time.
		List<Interval> intervals = KaplanMeierEstimator.compute(cases);
		log("Cases....: "+ cases.size());
		log("Intervals: "+ intervals.size());
		
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
		
		log("Interval in "+ (confidenceInterval * 100) +"%: "+ minutesToTest +" minutes until failure.");
		
		log("-------------------------------------------------------");
		log("DEFINING BASE PRICE");
		log("-------------------------------------------------------");
		
		// getting a median of last N days
		Calendar previousDateTemp = Calendar.getInstance();
		previousDateTemp.setTimeInMillis(baseTime.getTimeInMillis());
		previousDateTemp.add(Calendar.DAY_OF_MONTH, previousDaysToPriceReference * -1);
		
		List<MySpotPrice> changes = dao.findAll(REGION, ZONE, instance, previousDateTemp.getTime(), baseTime.getTime());
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
		
		float basePrice = Double.valueOf(mean).floatValue();
		log("Base price....: "+ String.format("%f", basePrice));
		
		
		log("-------------------------------------------------------");
		log("GETTING EXPERIMENT 'UNKNOWN' DATA AFTER BASE TIME "+ DateUtil.getFormattedDateTime(baseTime.getTime()));
		log("-------------------------------------------------------");
		
		Calendar infiniteDate = Calendar.getInstance();
		infiniteDate.add(Calendar.YEAR, 10);
		
		List<MySpotPrice> changesAfterBaseTime = dao.findAll(REGION, ZONE, instance, baseTime.getTime(), infiniteDate.getTime());
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
	
	
	
	@SuppressWarnings("deprecation")
	private void plotMatrix(Boolean mean, String title, List<MatrixRow>[][] matrixMedian) {
		List<Double> medians = new ArrayList<>();
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
				} else {
					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}

		StringBuilder filename = new StringBuilder();
		filename.append(instance);
		filename.append("-"+ (mean ? "mean" : "median"));
		filename.append("-"+ String.format(Locale.US, "%.2f", confidenceInterval));
		filename.append("-"+ previousDaysToPriceReference);
		filename.append("-"+ String.format(Locale.US, "%.2f", confidenceInterval));
		filename.append("-"+ String.format(Locale.US, "%.2f", Statistic.mean(medians)));
		filename.append("-"+ String.format(Locale.US, "%.2f", Statistic.coefficientOfVariation(medians)));
		filename.append(".txt");
		
		File parent = new File("plot", DateUtil.getFormattedDateTimeToFile(startedTime));
		if (!parent.exists()) {
			parent.mkdirs();
		}
		File file = new File(parent,  filename.toString());
		try {
			FileUtils.writeStringToFile(file, sb.toString());
		} catch (IOException e) {
			log.error("Error when trying to save file "+ file.toString() +": "+ e.getMessage());
		}
	}

	
	
	
	
	
	

	private void printMatrix(Boolean mean, String title, List<MatrixRow>[][] matrixMedian) {
		StringBuffer sb = new StringBuffer();
		sb.append(title + " - Confidence Interval: "+ String.format("%.2f", confidenceInterval) + System.lineSeparator());
		sb.append("   0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23  "+ System.lineSeparator());
		for (int i = 1; i < 8; i++) {
			sb.append(i +" ");
			for (int j = 0; j < 24; j++) {
				if (matrixMedian[i][j] != null && !matrixMedian[i][j].isEmpty()) {
					List<Double> values = matrixMedian[i][j].stream().map(x -> x.value).collect(Collectors.toList());
					sb.append(String.format(Locale.US, "%.2f", (mean ? Statistic.mean(values) : Statistic.median(values))) +" ");
					sb.append("("+ String.format("%4d", matrixMedian[i][j].get(0).minutes) +") ");
				} else {
					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
				}
			}
			sb.append(System.lineSeparator());
		}
		log.info(sb.toString());
	}
	
	@Override
	public void run() {
		try {
			this.dao = new SpotDAO();
			process();
		} catch (SQLException e) {
			log.error("Error when processing "+ instance +": "+ e.getMessage());
		}
	}
	
	public void log(String text) {
//		log.info(text);
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		
		int threadQtd = 8; //Runtime.getRuntime().availableProcessors();
		double[] confidenceInterval = new double[] {0.90, 0.95, 0.97};
		int[] daysToPriceReference = new int[] {1, 3, 7, 15};
		
		List<String> instancesToSimulate = AWSUtil.usedInstances;
//		instancesToSimulate.addAll(Arrays.asList(AWSUtil.usedInstances));
		
		instancesToSimulate.clear();
//		instancesToSimulate.add("m3.medium");
		instancesToSimulate.add("m3.large");
		
//		instancesToSimulate.add("r4.large");
//		instancesToSimulate.add("c3.large");
		
		ExecutorService executor = Executors.newFixedThreadPool(threadQtd);
		int threadsAddedCount = 0;
		for (String instance : instancesToSimulate) {
			for (Double confidence : confidenceInterval) {
				for (Integer daysBefore : daysToPriceReference) {
					executor.execute(new Thread(new SurvivalExperiment02Thread(instance, confidence, null, daysBefore, null, null), "Thread_"+ instance +"_"+ confidence));
					threadsAddedCount++;
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