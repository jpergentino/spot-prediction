package main.graphic;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.InstanceType;

import cloud.aws.core.AWSManager;
import core.db.MySQLDBConnection;
import core.exceptions.ParameterRequiredException;
import core.exceptions.TooManyConnectionsException;

public class GerarGraficoMudancasPrecos extends AbstractBaseTask {
	
	private Connection con;

	public GerarGraficoMudancasPrecos() throws TooManyConnectionsException {
		this.con = MySQLDBConnection.getInstance().getConnection();
		EXPORT_FOLDER += File.separatorChar + "PriceChanges_"+ new SimpleDateFormat("yy_MM_dd_HHmm").format(new Date());
		if (!new File(EXPORT_FOLDER).exists()) {
			new File(EXPORT_FOLDER).mkdirs();
		}
	}
	
	public void graphPriceChanges(Boolean perWeekDay, Boolean perHour, Boolean perDayShift, String region, String zone, String instance, Date dateInit, Date dateEnd ) {
		
		int paramValidateCount = 0;
		paramValidateCount += perWeekDay ? 1 : 0;
		paramValidateCount += perHour ? 1 : 0;
		paramValidateCount += perDayShift ? 1 : 0;
		
		// validating parameters
		if (paramValidateCount == 0 || paramValidateCount > 1) {
			throw new IllegalArgumentException("Provide perWeekDay, perHour or perDayShift only.");
		}
		
		paramValidateCount = 0;
		paramValidateCount += dateInit != null ? 1 : 0;
		paramValidateCount += dateEnd != null ? 1 : 0;
		
		if (paramValidateCount != 2 && paramValidateCount != 0) {
			throw new IllegalArgumentException("Provide both dates if some of them was provided. "+ paramValidateCount);
		}
		
		
		
		StringBuilder filename = new StringBuilder("price_changes"+ (perWeekDay ? "day" : perDayShift ? "day-shift" : "hour") );
		StringBuilder graphLabel = new StringBuilder("All price changes ");
		
		graphLabel.append( perWeekDay ? "per day" : perDayShift ? "per day-shift" : "per hour");
		graphLabel.append(System.lineSeparator());
		
		
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(perWeekDay ? " DAYOFWEEK(timestamp) " : "");
		sql.append(perHour ? " HOUR(timestamp) " : "");
		sql.append(perDayShift ? " CASE "
				+ " WHEN HOUR(timestamp) >= 0 AND HOUR(timestamp) <= 5 THEN 1 "
				+ " WHEN HOUR(timestamp) >= 6 AND HOUR(timestamp) <= 11 THEN 2 "
				+ " WHEN HOUR(timestamp) >= 12 AND HOUR(timestamp) <= 17 THEN 3 "
				+ " WHEN HOUR(timestamp) >= 18 AND HOUR(timestamp) <= 23 THEN 4 "
				+ " END " : "");
		sql.append(", timestamp, price from spotprice ");
		sql.append(" WHERE 1=1 ");
		
		filename.append("_"+ instance);
		if (region != null && !region.isEmpty()) {
			sql.append(" AND region = ? ");
			graphLabel.append(region +" ");
			filename.append("_"+ region);
		}
				
		if (zone != null && !zone.isEmpty()) {
			sql.append(" AND zone = ? ");
			graphLabel.append(zone +" ");
			filename.append("_"+ zone);
		}
		
		if (instance != null && !instance.isEmpty()) {
			sql.append(" AND instance = ? ");
			graphLabel.append(instance +" ");
		}
		
		if (dateInit != null || dateEnd != null) {
			sql.append(" AND DATE(timestamp) BETWEEN ? AND ? ");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			graphLabel.append(sdf.format(dateInit) +"/"+ sdf.format(dateEnd) +" ");
			filename.append("_"+ sdf.format(dateInit) +"-"+ sdf.format(dateEnd));
		}
		
		sql.append(" ORDER BY timestamp ");
		
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql.toString());
			
			int paramCount = 0;

			if (region != null && !region.isEmpty()) {
				ps.setString(++paramCount, region);
			}
					
			if (zone != null && !zone.isEmpty()) {
				ps.setString(++paramCount, zone);
			}
			
			if (instance != null && !instance.isEmpty()) {
				ps.setString(++paramCount, instance);
			}

			if (dateInit != null || dateEnd != null) {
				Calendar cInit = Calendar.getInstance();
				cInit.setTime(dateInit);
				cInit.set(Calendar.HOUR_OF_DAY, 0);
				
				Calendar cEnd = Calendar.getInstance();
				cEnd.setTime(dateEnd);
				cEnd.set(Calendar.HOUR_OF_DAY, 0);
				
				ps.setDate(++paramCount, new java.sql.Date(cInit.getTimeInMillis()));
				ps.setDate(++paramCount, new java.sql.Date(cEnd.getTimeInMillis()));
			}
			
			System.out.print("Generating graph: "+ graphLabel.toString().replaceAll(System.lineSeparator(), " ") +"...");
			
			ResultSet rs = ps.executeQuery();
			
			Map<Integer, Integer> mapPriceUp = new TreeMap<>();
			Map<Integer, Integer> mapPriceDown = new TreeMap<>();
			Map<Integer, Integer> mapPriceEquals = new TreeMap<>();
			Map<Integer, Integer> mapPriceChanges = new TreeMap<>();
			//initializing map
			int mapInit = perWeekDay || perDayShift ? 1 : 0; // 1 - weekDay (1-SUN, 2-MON, ...), dayShift (1-Dawn, 2-Morning) / 0 - perHour (0-23)  
			int mapSize = perWeekDay ? 7 : perDayShift ? 4 : 23;
			for (int i = mapInit; i <= mapSize; i++) {
				mapPriceUp.put(i, 0);
				mapPriceDown.put(i, 0);
				mapPriceEquals.put(i, 0);
				mapPriceChanges.put(i, 0);
			}
			
			double oldPrice = 0;
			
			int totalUpChanges = 0;
			int totalDownChanges = 0;
			int totalEqualsChanges = 0;
			int totalChanges = 0;
			
			while(rs.next()) {
				int reg = rs.getInt(1);
				double regPrice = rs.getDouble(3);
				
				
				if (regPrice == oldPrice) {
					mapPriceEquals.put(reg, mapPriceEquals.get(reg) + 1);
					totalEqualsChanges++;
				} else {
					mapPriceChanges.put(reg, mapPriceChanges.get(reg) + 1);
					
					if (regPrice > oldPrice) {
						mapPriceUp.put(reg, mapPriceUp.get(reg) + 1);
						totalUpChanges++;
					} else {
						mapPriceDown.put(reg, mapPriceDown.get(reg) + 1);
						totalDownChanges++;
					}
				}
				
				oldPrice = regPrice;
				totalChanges++;
			}
			
			if (totalChanges > 0) {
				
				XYSeriesCollection dataset = new XYSeriesCollection();
				XYSeries seriesUp = new XYSeries("UP", false, true);
				XYSeries seriesDown = new XYSeries("Down", false, true);
				
				mapPriceUp.entrySet().stream().forEach(x -> seriesUp.add(Integer.valueOf(x.getKey()), Integer.valueOf(x.getValue())));
				mapPriceDown.entrySet().stream().forEach(x -> seriesDown.add(x.getKey(), Integer.valueOf(x.getValue())));
				
				graphLabel.append(System.lineSeparator());
				graphLabel.append("Up/Down/Equals/Total: "+ totalUpChanges +"/"+ totalDownChanges +"/"+ totalEqualsChanges +"/"+ totalChanges);
				
				dataset.addSeries(seriesUp);
				dataset.addSeries(seriesDown);
				
				exportImageXYLineChart(dataset, filename.toString(), graphLabel.toString(), perWeekDay ? "Day of week" : perDayShift ? "Day-shift" : "Hour of day", "Number of changes", 1);
				
			}
			
			
			System.out.println("Done with "+ totalChanges +" rows and "+ totalUpChanges +" changes!");

			
		} catch (SQLException e) {
			System.out.println("Error on query: "+ e.getMessage() );
		} finally {
			try {
				ps.close();
			} catch (SQLException e1) {
				System.out.println("Error when trying to close statement/connection: "+ e1.getMessage() );
			}
		}
		
	}
	
	public void graphPriceChanges(String region, String zone, String instance, Date dateInit, Date dateEnd ) {
		
		int paramValidateCount = 0;
		paramValidateCount += dateInit != null ? 1 : 0;
		paramValidateCount += dateEnd != null ? 1 : 0;
		
		if (paramValidateCount != 2 && paramValidateCount != 0) {
			throw new IllegalArgumentException("Provide both dates if some of them was provided. "+ paramValidateCount);
		}
		
		StringBuilder filename = new StringBuilder("prices_");
		StringBuilder graphLabel = new StringBuilder("Prices over time ");
		
		graphLabel.append(System.lineSeparator());
		
		
		StringBuilder sql = new StringBuilder("SELECT ");
		sql.append(" timestamp, price ");
		sql.append(" FROM spotprice ");
		sql.append(" WHERE true ");
		
		if (region != null && !region.isEmpty()) {
			sql.append(" AND region = ? ");
			graphLabel.append(region +" ");
			filename.append("_"+ region);
		}
		
		if (zone != null && !zone.isEmpty()) {
			sql.append(" AND zone = ? ");
			graphLabel.append(zone +" ");
			filename.append("_"+ zone);
		}
		
		if (instance != null && !instance.isEmpty()) {
			sql.append(" AND instance = ? ");
			graphLabel.append(instance +" ");
			filename.append("_"+ instance);
		}
		
		if (dateInit != null || dateEnd != null) {
			sql.append(" AND DATE(timestamp) BETWEEN ? AND ? ");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			graphLabel.append(sdf.format(dateInit) +"/"+ sdf.format(dateEnd) +" ");
			filename.append("_"+ sdf.format(dateInit) +"-"+ sdf.format(dateEnd));
		}
		
		sql.append(" ORDER BY timestamp ");
		
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement(sql.toString());
			
			int paramCount = 0;
			
			if (region != null && !region.isEmpty()) {
				ps.setString(++paramCount, region);
			}
			
			if (zone != null && !zone.isEmpty()) {
				ps.setString(++paramCount, zone);
			}
			
			if (instance != null && !instance.isEmpty()) {
				ps.setString(++paramCount, instance);
			}
			
			if (dateInit != null || dateEnd != null) {
				Calendar cInit = Calendar.getInstance();
				cInit.setTime(dateInit);
				cInit.set(Calendar.HOUR_OF_DAY, 0);
				
				Calendar cEnd = Calendar.getInstance();
				cEnd.setTime(dateEnd);
				cEnd.set(Calendar.HOUR_OF_DAY, 0);
				
				ps.setDate(++paramCount, new java.sql.Date(cInit.getTimeInMillis()));
				ps.setDate(++paramCount, new java.sql.Date(cEnd.getTimeInMillis()));
			}
			
			System.out.print("Generating graph: "+ graphLabel.toString().replaceAll(System.lineSeparator(), " ") +"...");
			
			ResultSet rs = ps.executeQuery();
			
			Map<Long, Double> mapPriceChanges = new TreeMap<>();
			
			int totalChanges = 0;
			
			while(rs.next()) {
				Timestamp timestamp = rs.getTimestamp(1);
				double regPrice = rs.getDouble(2);
				
				mapPriceChanges.put(timestamp.getTime(), regPrice);
				totalChanges++;
			}
			
			saveCSVFile(mapPriceChanges, filename.toString());
			
			if (totalChanges > 0) {
				
				DefaultCategoryDataset dataset = new DefaultCategoryDataset();
				
				mapPriceChanges.entrySet().forEach(x -> {
					dataset.addValue(x.getValue(), "all changes", x.getKey());
				});
				
				graphLabel.append(System.lineSeparator());
				graphLabel.append("Total: "+ totalChanges);
				
				exportImageLineChart(dataset, filename.toString(), graphLabel.toString(), "Time", "Number of changes");
				
			}
			
			System.out.println("Done with "+ totalChanges +" rows and "+ totalChanges +" changes!");
			
			
		} catch (SQLException e) {
			System.out.println("Error on query: "+ e.getMessage() );
		} finally {
			try {
				ps.close();
			} catch (SQLException e1) {
				System.out.println("Error when trying to close statement/connection: "+ e1.getMessage() );
			}
		}
		
	}
	
	private void saveCSVFile(Map<Long, Double> mapPriceChanges, String filename) {
		FileWriter fw = null;
		BufferedWriter bw = null;
		
		File fileToSave = new File("/tmp/"+ filename +".csv");
		
		try {
			
			if (fileToSave.exists()) {
				FileUtils.forceDelete(fileToSave);
			}
			
			fw = new FileWriter(fileToSave, true);
			bw = new BufferedWriter(fw, 8192);
			
			int cont = 0;
			
			for(Entry<Long, Double> c : mapPriceChanges.entrySet()) {
				bw.write((cont++) +"\t"+ c.getValue() + System.lineSeparator());
			}
			
		} catch (IOException e) {
			System.out.println("Failed to save file "+ fileToSave.getAbsolutePath() +": "+ e.getMessage());
			System.exit(-1);
		} finally {
			try {
				if (bw != null) 
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException e) {
				System.out.println("Failed to close file "+ fileToSave.getAbsolutePath() +": "+ e.getMessage());
				System.exit(-1);
			}
		}
		
	}
	
	public static void main(String[] args) throws ParseException, ParameterRequiredException, TooManyConnectionsException {

		System.setProperty("user.timezone", "UTC");
		Calendar baseTime = Calendar.getInstance();
		baseTime.add(Calendar.MONTH, -4);
		Date dateInit = baseTime.getTime();
		
		GerarGraficoMudancasPrecos processData = new GerarGraficoMudancasPrecos();
		
		List<InstanceType> instances = Arrays.asList(InstanceType.values());
	    
		instances = Arrays.stream(AbstractBaseTask.usedInstances).map(x -> InstanceType.fromValue(x)).collect(Collectors.toList());
		
		Regions[] regions = Regions.values();
		regions = new Regions[]{Regions.US_WEST_1, Regions.US_WEST_2};

		
		for (Regions reg : regions) {
			
			try {
				
				if (Region.getRegion(reg).isServiceSupported(AmazonEC2.ENDPOINT_PREFIX)) {
					
					System.out.println("Region: "+ reg);
					
					AmazonEC2 ec2Exec = AWSManager.getInstance().getAmazonEC2(reg);
					
					List<AvailabilityZone> availabilityZones = ec2Exec.describeAvailabilityZones().getAvailabilityZones();
					
					for (AvailabilityZone zone : availabilityZones) {
						
						for (InstanceType instance : instances) {
							processData.graphPriceChanges(true, false, false, reg.getName(), zone.getZoneName(), instance.toString(), null, null);
							processData.graphPriceChanges(false, true, false, reg.getName(), zone.getZoneName(), instance.toString(), null, null);
							
//							processData.graphPriceChanges(reg.getName(), zone.getZoneName(), instance.toString(), dateInit, new Date());
						}
						
					}
					
				}
				
			} catch (AmazonEC2Exception e) {
				System.out.println("Error: "+ e.getMessage());
			}
			

			
		}
		
		Date dateEnd = Calendar.getInstance().getTime();
		
		System.out.println("Process init: "+ dateInit);
		System.out.println("Process end.: "+ dateEnd);
		
	}


}
