package core.util;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Statistic {

	public static double mean(List<Double> list) {
		double average = 0;
		for (double value : list) {
			average = average + value;
		}
		return (list.size() > 0 ? (average / list.size()) : 0);
	}

	public static double meanFromInteger(List<Integer> list) {
		return mean(list.stream().map(x -> x.doubleValue()).collect(Collectors.toList()));
	}
	
	public static double standardDeviationFromInteger(List<Integer> list) {
		return standardDeviation(list.stream().map(x -> x.doubleValue()).collect(Collectors.toList()));
	}

	public static double standardDeviation(List<Double> list) {
		return Math.sqrt(variance(list));
	}
	
	public static double median(List<Double> values) {
		
		double ret = 0;
		
		if (!values.isEmpty()) {
			
			Collections.sort(values);
			
			if (values.size() % 2 == 1) {
				ret = (Double) values.get((values.size() + 1) / 2 - 1);
			} else {
				int halfIndex = values.size() / 2;
				ret = (values.get(halfIndex - 1) + values.get(halfIndex)) / 2;
			}
			
		}
		
		return ret;
	}
	
	public static double medianFromInteger(List<Integer> values) {
		
		double ret = 0;
		
		if (!values.isEmpty()) {
			
			Collections.sort(values);
			
			if (values.size() % 2 == 1) {
				ret = values.get((values.size() + 1) / 2 - 1);
			} else {
				int halfIndex = values.size() / 2;
				ret = (values.get(halfIndex - 1) + values.get(halfIndex)) / 2;
			}
			
		}
		
		return ret;
	}
	
	public static double variance(List<Double> values) {

		double ret = 0;
		
		double average = mean(values);
		
		for (double number : values) {
			ret += Math.pow((number - average), 2);
		}
		
		return ret / values.size(); 
	}
	
	public static double coefficientOfVariation(List<Double> values) {
		return (standardDeviation(values) / mean(values) ) * 100; 
	}

	public static double coefficientOfVariationFromInteger(List<Integer> values) {
		return (standardDeviationFromInteger(values) / meanFromInteger(values) ) * 100; 
	}
	
}
