package main;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
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

public class SurvivalExperiment01 {
	
	private static final Logger log = LogManager.getLogger(SurvivalExperiment01.class);

	private static final String REGION = Regions.US_WEST_1.getName();
	private static final String ZONE = Regions.US_WEST_1.getName() + "b";
	
	static {
		System.setProperty("user.timezone", "UTC");
		System.setProperty("log4j.configurationFile", "log4j2.properties");
		log.debug("Properties configured");
	}

	
	public static void main(String[] args) throws SQLException {
		
		SpotDAO dao = new SpotDAO();
		
		@SuppressWarnings("unchecked")
		ArrayList<Double>[][] matrixMedian = new ArrayList[8][24];
		@SuppressWarnings("unchecked")
		ArrayList<Double>[][] matrixMean = new ArrayList[8][24];
		
		for (int i = 1; i < 8; i++) {
			for (int j = 0; j < 24; j++) {
				matrixMedian[i][j] = new ArrayList<>();
				matrixMean[i][j] = new ArrayList<>();
			}
		}
		
		
		for (String instance : AWSUtil.usedInstances) {
			
			LinkedList<Double> probabilities = new LinkedList<>();
			
			Calendar baseTime = Calendar.getInstance();
			baseTime.set(Calendar.MONTH, 8); // September
			baseTime.set(Calendar.DAY_OF_MONTH, 1); // 1st day
			baseTime.set(Calendar.HOUR_OF_DAY, 0); // 0 hour
			baseTime.set(Calendar.MINUTE, 0); // 0 minute
			baseTime.set(Calendar.SECOND, 0); // 0 second
			baseTime.set(Calendar.MILLISECOND, 0); // 0 second
			
			while (baseTime.get(Calendar.MONTH) < 9) {
				
				// Getting cases with same REGION, ZONE, INSTANCE, DAY and HOUR LIMIT 31/08/2017
				int dayOfWeek = baseTime.get(Calendar.DAY_OF_WEEK);
				int hourOfDay = baseTime.get(Calendar.HOUR_OF_DAY);
				List<Case> cases = dao.findCases(REGION, ZONE, instance, dayOfWeek, hourOfDay);
				List<Interval> intervals = KaplanMeierEstimator.compute(cases);
				log.info("-------------------------------------------------------");
				log.info("GETTING SIMILAR CASES. DOW: "+ dayOfWeek +" and HOD: "+ hourOfDay);
				log.info("-------------------------------------------------------");
				log.debug("Cases....: "+ cases.size());
				log.debug("Intervals: "+ intervals.size());
				
				// Interval in 0.95%: 1482 minutes until failure. (SurvivalExperiment01:81)
				double survivalPercentual = 0.95;
				Interval base = intervals.get(0);
				for (Interval n : intervals) {
					// log.info(n.getEnd() +" - "+ String.format("%f", n.getCumulativeSurvival()) +" "+ n.getNumberCensured() +"/"+ n.getNumberDied());
					if (n.getCumulativeSurvival() >= survivalPercentual) {
						base = n;
					} else {
						break;
					}
				}
				
				log.info("Interval in "+ (survivalPercentual * 100) +"%: "+ base.getEnd() +" minutes until failure.");
				
				log.info("-------------------------------------------------------");
				log.info("DEFINING BASE PRICE");
				log.info("-------------------------------------------------------");
				
				Calendar c1 = Calendar.getInstance();
				c1.setTimeInMillis(baseTime.getTimeInMillis());
				c1.add(Calendar.DAY_OF_MONTH, -1);
				
				List<MySpotPrice> changes = dao.findAll(REGION, ZONE, instance, c1.getTime(), baseTime.getTime());
				log.info("Price changes: "+ changes.size());
				List<Double> priceList = changes.stream().map(x -> Double.valueOf(x.getSpotPrice())).collect(Collectors.toList());
				
				double min = priceList.stream().reduce(Double.MAX_VALUE, Double::min);
				double max = priceList.stream().reduce(Double.MIN_VALUE, Double::max);
				
				double mean = Statistic.mean(priceList);
				double median = Statistic.median(priceList);
				double standardDeviation = Statistic.standardDeviation(priceList);
				log.info("Min price......: "+ String.format("%f", min));
				log.info("Max price......: "+ String.format("%f", max));
				log.info("Mean price.....: "+ String.format("%f", mean));
				log.info("Median price...: "+ String.format("%f", median));
				log.info("Standard Dev...: "+ String.format("%f", standardDeviation));
				
				
				log.info("-------------------------------------------------------");
				log.info("GETTING EXPERIMENT DATA SINCE "+ DateUtil.getFormattedDateTime(baseTime.getTime()));
				log.info("-------------------------------------------------------");
				
				float basePrice = Double.valueOf(median).floatValue();
				log.info("Base price....: "+ String.format("%f", basePrice));
				
				
				Calendar infiniteDate = Calendar.getInstance();
				infiniteDate.add(Calendar.YEAR, 10);
				
				List<MySpotPrice> changesSinceBaseTime = dao.findAll(REGION, ZONE, instance, baseTime.getTime(), infiniteDate.getTime());
				log.info("Price changes.: "+ changesSinceBaseTime.size());
				
				boolean died = false;
				for(MySpotPrice sp : changesSinceBaseTime) {
					
					
					if (sp.getSpotPriceFloat() > basePrice) {
						log.info("Starting at...: "+ DateUtil.getFormattedDateTime(baseTime.getTime()));
						log.info("Died at.......: "+ DateUtil.getFormattedDateTime(sp.getTimestamp()) +" with price $ "+ String.format("%f", sp.getSpotPriceFloat()));
						double durationInMinutes = TimeUnit.MILLISECONDS.toMinutes(sp.getTimestamp().getTime() - baseTime.getTimeInMillis());
						double probability = (durationInMinutes / base.getEnd()) > 1 ? 1 : (durationInMinutes / base.getEnd());
						log.info("Total alive...: "+ durationInMinutes +"/"+ base.getEnd() +" "+ String.format("%.2f", probability * 100)+ "%");
						died = true;
						probabilities.add(probability > 1 ? 1.0 : probability);
						break;
					}
				}
				
				if (!died) {
					probabilities.add(1.0);
					log.info("Not died.");
				}
				
				log.info("Mean..: "+ String.format("%.2f", Statistic.mean(probabilities)));
				log.info("Median: "+ String.format("%.2f", Statistic.median(probabilities)));
				
				baseTime.add(Calendar.HOUR_OF_DAY, 1);
				
				matrixMedian[dayOfWeek][hourOfDay].add(Statistic.median(probabilities));
				matrixMean[dayOfWeek][hourOfDay].add(Statistic.mean(probabilities));
				
				printMatrix(matrixMedian);
				printMatrix(matrixMean);
			
//				log.info("Press <ENTER> to continue...");
//				new Scanner(System.in).nextLine();
				
			} // while baseTime
			
			
		} // for instances
		
	}

	private static void printMatrix(ArrayList<Double>[][] matrix) {
		System.out.println("   0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15   16   17   18   19   20   21   22   23  ");
		
		for (int i = 1; i < 8; i++) {
			System.out.print(i +" ");
			for (int j = 0; j < 24; j++) {
				if (matrix[i][j] != null && !matrix[i][j].isEmpty()) {
					System.out.print(String.format("%.2f", Statistic.median(matrix[i][j])) +" ");
				} else {
					System.out.print(String.format("%.2f", Double.valueOf(0)) +" ");
				}
			}
			System.out.println();
		}
	}
	
}
