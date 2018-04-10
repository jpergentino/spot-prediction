package main.graphic;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;

import com.amazonaws.services.ec2.model.InstanceType;

import core.util.PropertiesUtil;

public abstract class AbstractBaseTask {
	
	private static final Logger log = LogManager.getLogger(AbstractBaseTask.class);
	
	//private static final int RES_H = 1920;
	//private static final int RES_V = 1080;
	
	private static final int[] RESOLUTION = new int[] {1280, 720};
	
//	private static final int RES_H_SQUARE = 640;
//	private static final int RES_V_SQUARE = 480;
	
	static {
		// load properties file
		propertiesUtil = PropertiesUtil.getInstance();
		System.setProperty("user.timezone", "UTC");
	}
	
	protected static PropertiesUtil propertiesUtil; 
	protected static String EXPORT_FOLDER = "/tmp/export";
	
	public static String[] usedInstances = new String[]{ 

			// General purpose
			InstanceType.M1Small.toString(),	// 1	1.7 	Moderate
			InstanceType.M3Medium.toString(),	// 1	3.75	Moderate
			InstanceType.M3Large.toString(), 	// 2	7.5		Moderate
			InstanceType.M4Large.toString(), 	// 4	16		High
			InstanceType.M3Xlarge.toString(),	// 4	15		Moderate
			
			// Compute Optimized
			InstanceType.C1Medium.toString(),	// 2	1.7		Moderate
			InstanceType.C3Large.toString(),	// 2	3.75	Moderate
			InstanceType.C4Xlarge.toString(),	// 4	7.50	Moderate
			
			// Memory Optimized
			InstanceType.R3Large.toString(),	// 2 	15		Moderate
			InstanceType.R4Large.toString(),	// 2 	15.25	10 Gbit
		};

	
	public AbstractBaseTask() {
		if (!new File(EXPORT_FOLDER).exists()) {
			new File(EXPORT_FOLDER).mkdirs();
		}
	}
	
	protected void exportImageLineChart(DefaultCategoryDataset dataset, String filename, String title, String x, String y) {
		
		File newFile = new File(EXPORT_FOLDER + File.separatorChar + filename +".png");
		try {
			JFreeChart chart = ChartFactory.createLineChart(title, x, y, dataset, PlotOrientation.VERTICAL, true, false, false);
			ChartUtilities.saveChartAsPNG(newFile, chart, RESOLUTION[0], RESOLUTION[1]);
			log.info("Saved file "+ newFile.getAbsolutePath());
		} catch (IOException e) {
			log.error("Failure to create line chart "+ newFile.getAbsolutePath());
			e.printStackTrace();
		}
	}
	
	protected void exportImageXYLineChart(XYDataset dataset, String filename, String title, String x, String y) {
		exportImageXYLineChart(dataset, filename, title, x, y, null);
	}
	
	protected void exportImageLineChart3D(CategoryDataset dataset, String filename, String title, String x, String y, Integer tickUnit) {
		
		File newFile = new File(EXPORT_FOLDER + File.separatorChar + filename +".png");
		try {
			JFreeChart chart = ChartFactory.createLineChart3D(title, x, y, dataset);
			
			if (tickUnit != null && tickUnit > 0) {
				NumberAxis xAxis = new NumberAxis();
				xAxis.setTickUnit(new NumberTickUnit(tickUnit));
				XYPlot plot = (XYPlot) chart.getPlot();
				plot.setDomainAxis(xAxis);
			}
			
			ChartUtilities.saveChartAsPNG(newFile, chart, RESOLUTION[0], RESOLUTION[1]);
			log.info("Saved file "+ newFile.getAbsolutePath());
		} catch (IOException e) {
			log.error("Failure to create XY line chart "+ newFile.getAbsolutePath());
			e.printStackTrace();
		}
	}
	
	protected void exportImageXYLineChart(XYDataset dataset, String filename, String title, String x, String y, Integer tickUnit) {
		
		File newFile = new File(EXPORT_FOLDER + File.separatorChar + filename +".png");
		try {
			JFreeChart chart = ChartFactory.createXYLineChart(title, x, y, dataset);
//			chart.setBackgroundPaint(Color.WHITE);
//			chart.getPlot().setBackgroundPaint(Color.WHITE);
			
			XYPlot plot = (XYPlot) chart.getPlot();
			
			if (tickUnit != null && tickUnit > 0) {
				NumberAxis xAxis = new NumberAxis();
				xAxis.setTickUnit(new NumberTickUnit(tickUnit));
				plot.setDomainAxis(xAxis);
			}
			
//			Smooth lines - Not works with a lot of data
//			XYSplineRenderer renderer = new XYSplineRenderer(10);
//			plot.setRenderer(renderer);
			
			ChartUtilities.saveChartAsPNG(newFile, chart, RESOLUTION[0], RESOLUTION[1]);
			log.info("Saved file "+ newFile.getAbsolutePath());
		} catch (IOException e) {
			log.error("Failure to create XY line chart "+ newFile.getAbsolutePath());
			e.printStackTrace();
		}
	}
	
	protected void exportImageTimeSeriesChart(XYDataset dataset, String filename, String title, String x, String y) {
		
		File newFile = new File(EXPORT_FOLDER + File.separatorChar + filename +".png");
		try {
			JFreeChart chart = ChartFactory.createTimeSeriesChart(title, x, y, dataset);
			ChartUtilities.saveChartAsPNG(newFile, chart, RESOLUTION[0], RESOLUTION[1]);
			log.info("Saved file "+ newFile.getAbsolutePath());
		} catch (IOException e) {
			log.error("Failure to create XY line chart "+ newFile.getAbsolutePath());
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unused")
	private void saveCSVFile(List<String> list, String filename) {
		FileWriter fw = null;
		BufferedWriter bw = null;
		
		File fileToSave = new File(EXPORT_FOLDER, filename +".csv");
		
		
		try {
			if (fileToSave.exists()) {
				FileUtils.forceDelete(fileToSave);
			}
			FileUtils.writeLines(fileToSave, list);
		} catch (IOException e) {
			log.error("Failed to save file: "+ fileToSave.getAbsolutePath());
		}
	}


}
