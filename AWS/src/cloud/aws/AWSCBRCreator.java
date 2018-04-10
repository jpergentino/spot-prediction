package cloud.aws;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;

import cloud.CaseBasedCreator;
import cloud.aws.bean.MySpotPrice;
import cloud.aws.dao.SpotDAO;
import core.executor.cbr.Case;
import core.util.DateUtil;

public class AWSCBRCreator extends Thread implements CaseBasedCreator, Runnable {
	
//	private static final double[] addictions = new double[] {1, 1.1, 1.2, 1.3, 1.4, 1.5};
	private static final double[] addictions = new double[] {1};

	private static final Logger log = LogManager.getLogger(AWSCBRCreator.class);
	
	private static SpotDAO dao;
	
	private String region;
	private String zone;
	private String instance;
	private File fileToSave;
	private Date initDate;
	private Date limitDate;
	
	public boolean saveToDatabase = true;
	
	public AWSCBRCreator(String region, String zone, String instance, Date initDate, Date limitDate, File fileToSave, boolean saveToDB) {
		this(region, zone, instance, fileToSave, saveToDB);

		this.initDate = initDate;
		this.limitDate = limitDate;
	}
	
	public AWSCBRCreator(String region, String zone, String instance, File fileToSave, boolean saveToDB) {
		
		this.region = region;
		this.zone = zone;
		this.instance = instance;
		this.fileToSave = fileToSave;
		this.saveToDatabase = saveToDB;
		
	}
	
	public void executeOld() {
		
		log.info("Creating cases from "+ region +"/"+ zone +"/"+ instance);
		
		List<MySpotPrice> listOfChanges = null;
		try {
			listOfChanges = dao.findAll(region, zone, instance);
		} catch (SQLException e) {
			log.error("Error when trying to create case based instances: "+ e.getMessage());
		}
		
		log.info("Records found: "+ (listOfChanges != null ? listOfChanges.size() : 0) +". Generating cases.");
		
		if (listOfChanges != null && !listOfChanges.isEmpty()) {
			
			List<Case> caseBasedList = new LinkedList<>();
			
			Calendar baseTime = Calendar.getInstance();
			double basePrice = 0;
			
			MySpotPrice lastRow = listOfChanges.get(listOfChanges.size() -1);
			
			for (double addiction : addictions) {
				
				log.debug("Processing with addiction of "+ addiction);
				
				for (int i = 0; i < listOfChanges.size(); i++) {
					
					MySpotPrice row1 = listOfChanges.get(i);
					
					baseTime.setTimeInMillis(row1.getTimestamp().getTime());
					basePrice = Double.valueOf(row1.getSpotPrice()) * addiction;
					
					int skip = 0;
					boolean isCensured = true;
					
					for (int j = i+1; j < listOfChanges.size(); j++) {
						
						MySpotPrice row2 = listOfChanges.get(j);
						
						double comparablePrice = Double.valueOf(row2.getSpotPrice());
						
						if (comparablePrice > basePrice) {
							
							isCensured = false;
							
							int minutesBetween = Minutes.minutesBetween(new DateTime(baseTime.getTimeInMillis()), new DateTime(row2.getTimestamp().getTime())).getMinutes();
							
							if (minutesBetween >= 60) {
								
								Case newCase = new Case(region, zone, instance);
								newCase.setDayOfWeek(baseTime.get(Calendar.DAY_OF_WEEK));
								newCase.setHourOfDay(baseTime.get(Calendar.HOUR_OF_DAY));
								newCase.setAdditionAllowed(addiction);
								newCase.setTimeToRevocation(minutesBetween);
								newCase.setSkipRecords(skip);
								newCase.setInitTime(row1.getTimestamp().getTime());
								newCase.setEndTime(row2.getTimestamp().getTime());
								newCase.setInitValue(basePrice);
								newCase.setEndValue(comparablePrice);
								newCase.setCensored(false);
								
								caseBasedList.add(newCase);
								
							}
							break;
						}
						
						skip++;
					}
					
					if (isCensured) {
						
						int minutesBetween = Minutes.minutesBetween(new DateTime(baseTime.getTimeInMillis()), new DateTime(lastRow.getTimestamp().getTime())).getMinutes();
						
						if (minutesBetween >= 60) {
							
							Case newCase = new Case(region, zone, instance);
							newCase.setDayOfWeek(baseTime.get(Calendar.DAY_OF_WEEK));
							newCase.setHourOfDay(baseTime.get(Calendar.HOUR_OF_DAY));
							newCase.setAdditionAllowed(addiction);
							newCase.setTimeToRevocation(minutesBetween);
							newCase.setSkipRecords(skip);
							newCase.setInitTime(baseTime.getTimeInMillis());
							newCase.setEndTime(lastRow.getTimestamp().getTime());
							newCase.setInitValue(basePrice);
							newCase.setEndValue(Double.valueOf(lastRow.getSpotPrice()));
							newCase.setCensored(true);
							
							caseBasedList.add(newCase);
						}
					}
					
				}
				
				log.info(caseBasedList.size() +" cases generated from "+ (listOfChanges != null ? listOfChanges.size() : 0) +" records in "+ region +"/"+ zone +"/"+ instance );
				saveCasesToFile(caseBasedList);
				
			}
			
			
			
		}
		
		
	}
	
	public void executeNew() throws SQLException {
		
		StringBuilder st = new StringBuilder("### Creating cases from "+ region +"/"+ zone +"/"+ instance +". ");
		
		if (this.initDate != null || this.limitDate != null) {
			SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_PATTERN);
			st.append("Dates: "+ sdf.format(initDate) +"- "+ sdf.format(limitDate));
		}
		log.info(st.toString());
		
		List<MySpotPrice> listOfChanges = null;
		try {
			if (this.initDate != null || this.limitDate != null) {
				listOfChanges = dao.findAll(this.region, this.zone, this.instance, this.initDate, this.limitDate);
			} else {
				listOfChanges = dao.findAll(this.region, this.zone, this.instance);
			}
		} catch (SQLException e) {
			log.error("Error when trying to create case based instances: "+ e.getMessage());
		}
		
		if (listOfChanges != null && !listOfChanges.isEmpty()) {
			
			log.info("Records found: "+ (listOfChanges != null ? listOfChanges.size() : 0) +". Generating cases.");
			
			List<Case> caseBasedList = new LinkedList<>();
			
			int skip = 0;
			
			MySpotPrice lastRow = listOfChanges.get(listOfChanges.size() -1);
			
			for (double addiction : addictions) {
				
				log.debug("Processing with addiction of "+ addiction);
				
				for (int i = 0; i < listOfChanges.size(); i++) {
					
					MySpotPrice row1 = listOfChanges.get(i);
					
					Calendar baseTime = Calendar.getInstance();
					baseTime.setTimeInMillis(row1.getTimestamp().getTime());
					double basePrice = Double.valueOf(row1.getSpotPrice()) * addiction;
					
					skip = 0;
					boolean isCensured = true;
					
					for (int j = i+1; j < listOfChanges.size(); j++) {
						
						skip++;
						MySpotPrice row2 = listOfChanges.get(j);
						
						Calendar comparableTime = Calendar.getInstance();
						comparableTime.setTimeInMillis(row2.getTimestamp().getTime());
						double comparablePrice = Double.valueOf(row2.getSpotPrice());
						
						if (comparablePrice > basePrice) {
							
							isCensured = false;
							
							int minutesBetween = Minutes.minutesBetween(new DateTime(baseTime.getTimeInMillis()), new DateTime(comparableTime.getTimeInMillis())).getMinutes();
							
							if (minutesBetween >= 60) {
								
								Case newCase = new Case(region, zone, instance);
								newCase.setDayOfWeek(baseTime.get(Calendar.DAY_OF_WEEK));
								newCase.setHourOfDay(baseTime.get(Calendar.HOUR_OF_DAY));
								newCase.setAdditionAllowed(addiction);
								newCase.setTimeToRevocation(minutesBetween);
								newCase.setSkipRecords(skip);
								newCase.setInitValue(basePrice);
								newCase.setEndValue(comparablePrice);
								newCase.setInitTime(baseTime.getTimeInMillis());
								newCase.setEndTime(comparableTime.getTimeInMillis());
								newCase.setCensored(false);
								
								caseBasedList.add(newCase);
								
								boolean newCasesExists = true;
								
								Calendar newBaseTime = (Calendar) baseTime.clone();
								
								while (newCasesExists) {
									
									newBaseTime.add(Calendar.HOUR, 1);
									
									minutesBetween = Minutes.minutesBetween(new DateTime(newBaseTime.getTimeInMillis()), new DateTime(comparableTime.getTimeInMillis())).getMinutes();
									if (minutesBetween >= 60) {
										
										Case newCaseOverTime = new Case(region, zone, instance);
										newCaseOverTime.setDayOfWeek(newBaseTime.get(Calendar.DAY_OF_WEEK));
										newCaseOverTime.setHourOfDay(newBaseTime.get(Calendar.HOUR_OF_DAY));
										newCaseOverTime.setAdditionAllowed(0);
										newCaseOverTime.setTimeToRevocation(minutesBetween);
										newCaseOverTime.setSkipRecords(0);
										newCaseOverTime.setInitValue(basePrice);
										newCaseOverTime.setEndValue(comparablePrice);
										newCaseOverTime.setInitTime(newBaseTime.getTimeInMillis());
										newCaseOverTime.setEndTime(comparableTime.getTimeInMillis());
										newCaseOverTime.setCensored(false);
										
										caseBasedList.add(newCaseOverTime);
										
									} else {
										newCasesExists = false;
									}
									
								}
								
							}
							break;
						}
						
					}
					
					if (isCensured) {
						
						int minutesBetween = Minutes.minutesBetween(new DateTime(baseTime.getTimeInMillis()), new DateTime(lastRow.getTimestamp().getTime())).getMinutes();
						
						if (minutesBetween >= 60) {
							
							Case newCase = new Case(region, zone, instance);
							newCase.setInstance(instance);
							newCase.setDayOfWeek(baseTime.get(Calendar.DAY_OF_WEEK));
							newCase.setHourOfDay(baseTime.get(Calendar.HOUR_OF_DAY));
							newCase.setAdditionAllowed(addiction);
							newCase.setTimeToRevocation(minutesBetween);
							newCase.setSkipRecords(skip);
							newCase.setInitTime(baseTime.getTimeInMillis());
							newCase.setEndTime(lastRow.getTimestamp().getTime());
							newCase.setInitValue(basePrice);
							newCase.setEndValue(Double.valueOf(lastRow.getSpotPrice()));
							newCase.setCensored(true);
							
							caseBasedList.add(newCase);
						}
					}
					
				}
				
				log.info(caseBasedList.size() +" cases generated from "+ (listOfChanges != null ? listOfChanges.size() : 0) +" records in "+ region +"/"+ zone +"/"+ instance );
				if (saveToDatabase) {
					saveCasesToBD(caseBasedList);
				} else {
					saveCasesToFile(caseBasedList);
				}
				caseBasedList.clear();
				
			}
			
			
		}
		
		log.info("### End of processing "+ region +"/"+ zone +"/"+ instance);
		
		
	}
	
	private void saveCasesToBD(List<Case> caseBasedList) throws SQLException {
		dao.saveSpotCasesBatch(caseBasedList);
	}

	public void saveCasesToFile(List<Case> caseList) {
		
		if (caseList != null && !caseList.isEmpty()) {
			
			FileWriter fw = null;
			BufferedWriter bw = null;
			
			try {
				
				if (!fileToSave.exists()) {
//				bw.write(Case.toStringHead() + System.lineSeparator());
					FileUtils.writeStringToFile(fileToSave, Case.toStringHead() + System.lineSeparator(), StandardCharsets.UTF_8);
				}
//			FileUtils.writeLines(file, caseList.stream().map(x -> x.toStringCase()).collect(Collectors.toList()), true);
				
				fw = new FileWriter(fileToSave, true);
				bw = new BufferedWriter(fw, 8192);
				
				for(Case c : caseList) {
					bw.write(c.toStringCase() + System.lineSeparator());
				}
				
				log.debug("File saved: "+ fileToSave.getAbsolutePath());
			} catch (IOException e) {
				log.error("Failed to save file "+ fileToSave.getAbsolutePath() +": "+ e.getMessage());
			} finally {
				try {
					if (bw != null) 
						bw.close();
					if (fw != null)
						fw.close();
				} catch (IOException e) {
					log.error("Failed to close file "+ fileToSave.getAbsolutePath() +": "+ e.getMessage());
				}
			}
			
		}
		
		
	}
	
	@Override
	public void execute() throws SQLException {
		executeNew();
	}
	
	@Override
	public void run() {
		try {
			dao = new SpotDAO();
			execute();
			dao.closeConnection();
		} catch (SQLException e) {
			log.error("Error when trying to execute AWSCRBCreator in "+ region +"/"+ zone +"/"+ instance+ ": "+ e.getMessage());
			e.printStackTrace();
		}
	}
	
}