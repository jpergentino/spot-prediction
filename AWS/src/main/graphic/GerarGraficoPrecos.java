package main.graphic;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.Second;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import com.amazonaws.regions.Regions;

import cloud.aws.dao.SpotDAO;

public class GerarGraficoPrecos extends AbstractBaseTask {
	
	private static final Logger log = LogManager.getLogger(GerarGraficoPrecos.class);
	
	private SpotDAO dao;
	
	public GerarGraficoPrecos() {
		dao = new SpotDAO();
	}

	public void run() throws SQLException, IOException {
		
		String region = Regions.US_WEST_1.getName();
		String[] zones = new String[] {"us-west-1b", "us-west-1c", };
		String[] instances = new String[] {"m4.large", "m3.medium", "m3.xlarge", "r4.large", "m3.xlarge"};
		
		for (String zone : zones) {
			
			for (String instance : instances) {
				TimeSeriesCollection dataset = new TimeSeriesCollection();
				TimeSeries series = new TimeSeries("Prices");
				
				Calendar calInit = Calendar.getInstance();
				calInit.add(Calendar.MONTH, -1);
				calInit.set(Calendar.MONTH, 8);
				calInit.set(Calendar.DAY_OF_MONTH, 5);
				
				Calendar calEnd = Calendar.getInstance();
				calEnd.set(Calendar.MONTH, 8);
				calEnd.set(Calendar.DAY_OF_MONTH, 10);
				
				Map<Long, Double> map = dao.findPriceChangeHistory(region, zone, instance, calInit.getTime(), calEnd.getTime());
				
				for (Map.Entry<Long, Double> entry : map.entrySet()) {
					Calendar cal = Calendar.getInstance();
					cal.setTimeInMillis(entry.getKey());
//					RegularTimePeriod timePeriod = new Day(cal.getTime());
//					RegularTimePeriod timePeriod = new Minute(cal.getTime());
					RegularTimePeriod timePeriod = new Second(cal.getTime());
					series.addOrUpdate(timePeriod, entry.getValue());
				}
				
				dataset.addSeries(series);
				
				SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
				
				String filename = "prices_"+ region + "_"+ zone +"_"+ sdf.format(calInit.getTime()) +"-"+ sdf.format(calEnd.getTime()) +"_"+ instance;
				String title = "Price changes "+ region + "/"+ zone +" "+ instance +" "+ sdf.format(calInit.getTime()) +" to "+ sdf.format(calEnd.getTime()) +" ("+ series.getItemCount() +")";
				super.exportImageTimeSeriesChart(dataset, filename, title, "Date", "Price");
				
			}
			
			log.info("Done.");
			
		}
		

	}

	public static void main(String[] args) throws SQLException, IOException {
		new GerarGraficoPrecos().run();
	}

}
