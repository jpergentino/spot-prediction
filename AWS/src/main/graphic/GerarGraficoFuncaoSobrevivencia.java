package main.graphic;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import cloud.aws.dao.SpotDAO;
import cloud.aws.util.AWSUtil;
import core.executor.cbr.Case;
import core.util.KaplanMeierEstimator;
import core.util.KaplanMeierEstimator.Interval;

public class GerarGraficoFuncaoSobrevivencia extends AbstractBaseTask {
	
	private static final Logger log = LogManager.getLogger(GerarGraficoFuncaoSobrevivencia.class);
	
	private SpotDAO dao;
	
	public GerarGraficoFuncaoSobrevivencia() {
		dao = new SpotDAO();
		EXPORT_FOLDER += File.separatorChar + "SurvivorCurve_"+ new SimpleDateFormat("yy_MM_dd_HHmm").format(new Date());
		if (!new File(EXPORT_FOLDER).exists()) {
			new File(EXPORT_FOLDER).mkdirs();
		}

	}

	public void run() throws SQLException, IOException {
		
		String[] regions = AWSUtil.usedRegions;
		String[] zones = AWSUtil.usedZones; 
		List<String> instances = AWSUtil.usedInstances;
		int[] days = IntStream.rangeClosed(1, 7).toArray();
		int[] hours = IntStream.rangeClosed(0, 23).toArray();
		for (int i = 0; i < hours.length; i++) {
			hours[i] = i;
		}
		
		log.info("Generating survivor graphic with:");
		log.info("Instances: "+ instances);
		log.info("Days.....: "+ Arrays.toString(days));
		log.info("Hours....: "+ Arrays.toString(hours));
		
		
		for (int d = 0; d < days.length; d++) {
			
			for (int h = 0; h < hours.length; h++) {
				
				XYSeriesCollection dataset = new XYSeriesCollection();
				
				int dayOfWeek = days[d];
				int hourOfDay = hours[h];
				
				boolean axisInHour = true;
				
				for (String region : regions) {
					for (String zone : zones) {
						for (String instance : instances) {
							try {
								XYSeries series = new XYSeries(instance);
								recoverCases(series, region, zone, instance, dayOfWeek, hourOfDay, axisInHour);
								dataset.addSeries(series);
							} catch (Exception e) {
								log.error("Error when recover cases "+ e.getMessage());
							}
						}
						String labelX = "Time in "+ (axisInHour ? "hours" : "minutes");
						String labelY = "Survival analysis (%)";
						
						String filename = region +"_"+ zone +"_"+ dayOfWeek +"_"+ hourOfDay;
						super.exportImageXYLineChart(dataset, filename, "Survival curve "+ region +"/"+ zone +"\nWeekday: "+ dayOfWeek + " Hour: "+ hourOfDay, labelX, labelY);
					}
					
				}
				
				
			}
			
		}		


	}

	private void recoverCases(XYSeries seriesNew, String region, String zone, String instance, int dayOfWeek, int hourOfDay, boolean axisInHour) throws SQLException {
		List<Case> casesList = dao.findCases(region, zone, instance, dayOfWeek, hourOfDay);
		log.info("["+ instance + " "+ dayOfWeek +" "+ hourOfDay +"]\t Case Size: "+ casesList.size());
		List<Interval> intervals = KaplanMeierEstimator.compute(casesList);
		for (Interval i : intervals) {
			seriesNew.add(i.getEnd() / (axisInHour ? 60 : 1), i.getCumulativeSurvival());
		}
	}

	public static void main(String[] args) throws SQLException, IOException {
		new GerarGraficoFuncaoSobrevivencia().run();
	}

}
