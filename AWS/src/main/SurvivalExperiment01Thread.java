package main;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

public class SurvivalExperiment01Thread implements Runnable {
	
	private static final Logger log = LogManager.getLogger(SurvivalExperiment01Thread.class);

	private static final String REGION = Regions.US_WEST_1.getName();
	private static final String ZONE = Regions.US_WEST_1.getName() + "b";
	
	private String instance;
	
	// Confidence Interval in 0.95%
	double confidenceInterval = 0.95;
	
	static {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		log.debug("Properties configured");
	}
	
	public SurvivalExperiment01Thread(String instance) {
		this.instance = instance;
	}
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		
		int threadQtd = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors.newFixedThreadPool(threadQtd);
		
		int threadsAddedCount = 0;
		List<String> instancesToSimulate = AWSUtil.usedInstances;
//		instancesToSimulate.addAll(Arrays.asList(AWSUtil.usedInstances));
		
		instancesToSimulate.clear();
		instancesToSimulate.add("m3.medium");
		instancesToSimulate.add("m3.large");
		instancesToSimulate.add("c3.large");
		
		for (String instance : instancesToSimulate) {
			executor.execute(new Thread(new SurvivalExperiment01Thread(instance), "Thread_"+ instance));
			threadsAddedCount++;
		}
		
		executor.shutdown();
        while (!executor.isTerminated()) {
        	
        	Thread.sleep(60 * 1000); // 1 minute
        	
        	if (executor instanceof ThreadPoolExecutor) {
        	    String queue = ((ThreadPoolExecutor) executor).getQueue().stream().map(r -> (Thread) r).map(Thread::getName).collect(Collectors.joining(","));
        	    long completed = ((ThreadPoolExecutor) executor).getCompletedTaskCount();
				log.debug("Pool status ("+ String.format("%02d", completed) +"/"+ String.format("%02d", threadsAddedCount) +"): Queue: "+ queue);
        	}
        	
        }

		log.debug("Done.");
		
	}


	
	public void process() throws SQLException {
		
		SpotDAO dao = new SpotDAO();
		
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
			
			int dayOfWeek = baseTime.get(Calendar.DAY_OF_WEEK);
			int hourOfDay = baseTime.get(Calendar.HOUR_OF_DAY);
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
//				 log(n.getEnd() +" - "+ String.format("%f", n.getCumulativeSurvival()) +" "+ n.getNumberCensured() +"/"+ n.getNumberDied());
				if (n.getCumulativeSurvival() >= confidenceInterval) {
					base = n;
				} else {
					break;
				}
			}
			int baseMinutesUntilFailure = base.getEnd();
			log("Interval in "+ (confidenceInterval * 100) +"%: "+ baseMinutesUntilFailure +" minutes until failure.");
			
			log("-------------------------------------------------------");
			log("DEFINING BASE PRICE");
			log("-------------------------------------------------------");
			
			// getting a median of last N days
			Calendar previousDateTemp = Calendar.getInstance();
			previousDateTemp.setTimeInMillis(baseTime.getTimeInMillis());
			previousDateTemp.add(Calendar.DAY_OF_MONTH, -1);
			
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
			
			float basePrice = Double.valueOf(median).floatValue();
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
					double probability = durationInMinutes / baseMinutesUntilFailure;
					log("Total alive: "+ durationInMinutes +"/"+ baseMinutesUntilFailure +" "+ String.format("%.2f", probability * 100)+ "%");
					died = true;
					actualProbability = probability > 1 ? 1.0 : probability;
					break;
				}
			}
			
			if (!died) {
				actualProbability = 1.0;
				log("Not died.");
			}
			
			baseTime.add(Calendar.HOUR_OF_DAY, 1);
			
			matrixMedian[dayOfWeek][hourOfDay].add(new MatrixRow(actualProbability, baseMinutesUntilFailure));
			matrixMean[dayOfWeek][hourOfDay].add(new MatrixRow(actualProbability, baseMinutesUntilFailure));
		
			
		} // while baseTime

		printMatrix(false, "Median on "+ instance, matrixMedian);
		printMatrix(true, "Mean on "+ instance, matrixMean);
		
		
	}

//	private void printMatrix(Boolean mean, String title, ArrayList<Double>[][] matrix) {
//		StringBuffer sb = new StringBuffer();
//		sb.append(title + " - Confidence Interval: "+ String.format("%.2f", confidenceInterval) + System.lineSeparator());
//		sb.append("   1    2    3    4    5    6    7   "+ System.lineSeparator());
//		for (int i = 0; i <= 23; i++) {
//			sb.append(i +" ");
//			for (int j = 1; j <= 7; j++) {
//				if (matrix[j][i] != null && !matrix[j][i].isEmpty()) {
//					sb.append(String.format(Locale.US, "%.2f", (mean ? Statistic.mean(matrix[j][i]) : Statistic.median(matrix[j][i]))) +" ");
//				} else {
//					sb.append(String.format(Locale.US, "%.2f", Double.valueOf(0)) +" ");
//				}
//			}
//			sb.append(System.lineSeparator());
//		}
//		log.info(sb.toString());
//	}

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
			process();
		} catch (SQLException e) {
			log.error("Error when processing "+ instance);
			e.printStackTrace();
		}
	}
	
	public void log(String text) {
//		log.info(text);
	}
	
}