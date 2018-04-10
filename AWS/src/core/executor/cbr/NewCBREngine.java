package core.executor.cbr;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.ec2.model.InstanceType;

import de.dfki.mycbr.core.DefaultCaseBase;
import de.dfki.mycbr.core.Project;
import de.dfki.mycbr.core.casebase.Instance;
import de.dfki.mycbr.core.model.Concept;
import de.dfki.mycbr.core.model.IntegerDesc;
import de.dfki.mycbr.core.model.SymbolDesc;
import de.dfki.mycbr.core.retrieval.Retrieval;
import de.dfki.mycbr.core.similarity.IntegerFct;
import de.dfki.mycbr.core.similarity.Similarity;
import de.dfki.mycbr.core.similarity.SymbolFct;
import de.dfki.mycbr.core.similarity.config.DistanceConfig;
import de.dfki.mycbr.core.similarity.config.NumberConfig;
import de.dfki.mycbr.io.CSVImporter;
import de.dfki.mycbr.util.Pair;

public final class NewCBREngine {
	
	public static final String ATTR_NAME_EXECUTION_HOUR = "executionHourAttr";
	public static final String ATTR_NAME_EXECUTION_DAY = "executionDayAttr";
	public static final String ATTR_NAME_EXECUTION_TIME = "executionTimeAttr";
	public static final String ATTR_NAME_INSTANCE = "instance";
	
	private static final String CASEBASE_DATABASE_NAME = "AWSCaseBase";
	private static final String CONCEPT_NAME = "TaskExecution";

	private final Logger log = LogManager.getLogger(getClass());
	
	private static NewCBREngine uniqueInstance = null;
	
	private Project p;
	private Concept concept;
	private DefaultCaseBase database;
	
	// Attributes
	private SymbolDesc instanceAttr;
	private IntegerDesc executionDayAttr;
	private IntegerDesc executionHourAttr;
	private IntegerDesc executionTimeAttr;
	
	private int instancesCount = 0; 
	
	public static NewCBREngine getInstance(String path) {
		if (uniqueInstance == null) {
//			uniqueInstance = new NewCBREngine();
			uniqueInstance = new NewCBREngine(path);
		}
		return uniqueInstance;
	}
	
	private NewCBREngine() {
		createEmptyProject();
	}

	private NewCBREngine(String path) {
		createProjectFromFile(path);
	}
	
	public void createProjectFromFile(String path) {
		
	}
	
	
	public void createEmptyProject() {
		try {
			p = new Project();
			concept = p.createTopConcept(CONCEPT_NAME);
			database = p.createDefaultCB(CASEBASE_DATABASE_NAME);
			
			// add attributes
			List<String> instanceList = Arrays.stream(InstanceType.values()).map(InstanceType::toString).collect(Collectors.toList());
			instanceAttr = new SymbolDesc(concept, ATTR_NAME_INSTANCE, new HashSet<String>(instanceList));
			executionDayAttr = new IntegerDesc(concept, ATTR_NAME_EXECUTION_DAY, 0, 7);
			executionHourAttr = new IntegerDesc(concept, ATTR_NAME_EXECUTION_HOUR, 0, 23);
			executionTimeAttr = new IntegerDesc(concept, ATTR_NAME_EXECUTION_TIME, 1, 60 * 24 * 7 * 4 * 12); // max for a year (40320)
			
			// add similarities functions
			SymbolFct instanceFunction = instanceAttr.addSymbolFct("SimInstanceFunction", true);
			for(String inst : instanceList) {
				instanceFunction.setSimilarity(inst, inst, Similarity.get(1d));
			}
			
			IntegerFct executionDayFunction = executionDayAttr.addIntegerFct("SimExecutionDayFunction", true);
			executionDayFunction.setSymmetric(true);
			executionDayFunction.setDistanceFct(DistanceConfig.DIFFERENCE);
			executionDayFunction.setFunctionTypeL(NumberConfig.SMOOTH_STEP_AT);
			executionDayFunction.setFunctionTypeR(NumberConfig.SMOOTH_STEP_AT);
			executionDayFunction.setFunctionParameterL(0);
			executionDayFunction.setFunctionParameterR(0);
			
			IntegerFct executionHourFunction = executionHourAttr.addIntegerFct("SimExecutionHourFunction", true);
			executionHourFunction.setSymmetric(true);
			executionHourFunction.setDistanceFct(DistanceConfig.DIFFERENCE);
			executionHourFunction.setFunctionTypeL(NumberConfig.POLYNOMIAL_WITH);
			executionHourFunction.setFunctionTypeR(NumberConfig.POLYNOMIAL_WITH);
			executionHourFunction.setFunctionParameterL(1);
			executionHourFunction.setFunctionParameterR(1);
			
			IntegerFct executionTimeFunction = executionTimeAttr.addIntegerFct("SimExecutionTimeFunction", true);
			executionTimeFunction.setSymmetric(true);
			executionTimeFunction.setDistanceFct(DistanceConfig.DIFFERENCE);
			executionTimeFunction.setFunctionTypeL(NumberConfig.POLYNOMIAL_WITH);
			executionTimeFunction.setFunctionTypeR(NumberConfig.POLYNOMIAL_WITH);
			executionTimeFunction.setFunctionParameterL(1);
			executionTimeFunction.setFunctionParameterR(1);
			
			
		} catch (Exception e) {
			log.error("Failure when trying to create a project: "+ e.getMessage());
		}
	}

	public boolean addCase(List<Case> caseList)  {
		
		boolean add = true;
		
		for (Case caseObj : caseList) {
			if (!addCase(caseObj)) {
				add = false;
				break;
			}
		}
		if (!add) {
			clearDatabase();
		}
		return add;
	}
	
	public boolean addCase(Case obj)  {
		try {
			boolean addError = false;
			Instance i = concept.addInstance("Exec_"+ (++instancesCount));
			i.addAttribute(instanceAttr, instanceAttr.getAttribute(obj.getInstance()));
			if (!i.addAttribute(executionDayAttr, obj.getDayOfWeek())) {
				log.error("Failed adding "+ i.getName() +" "+ executionDayAttr.getName() +": "+ obj.getDayOfWeek());
				addError = true;
			}
			if (!i.addAttribute(executionHourAttr, obj.getHourOfDay())) {
				log.error("Failed adding "+ i.getName() +" "+ executionHourAttr.getName() +": "+ obj.getHourOfDay());
				addError = true;
			}
			if (!i.addAttribute(executionTimeAttr, obj.getTimeToRevocation())) {
				log.error("Failed adding "+ i.getName() +" "+ executionTimeAttr.getName() +": "+ obj.getTimeToRevocation());
				addError = true;
			}
//			i.getAttributes().entrySet().stream().forEach(x -> log.debug("Adding: "+ x.getKey().getName() +"="+ x.getValue().getValueAsString()));
			if (!addError) {
				database.addCase(i);
			} else {
				log.error("Failed adding case "+ obj);
			}
		} catch (Exception e) {
			log.error("Error when trying to add case: "+ e.getMessage());
			return false;
		}
		
		return true;
	}
	
	public boolean saveCaseDatabaseToFile(File file) {
		
		
		if (file.exists()) {
			log.debug("File exists: "+ file.getAbsolutePath());
			boolean result = file.delete();
			log.debug("Deleted "+ file.getAbsolutePath() +"? "+ result);
		}
		
		Collection<Instance> cases = database.getCases();
		
		List<String> lines = new ArrayList<>(cases.size());
		cases.forEach(x -> {
			System.out.println("Adding "+ x);
			lines.add(new Case().fromInstance(x).toStringCase());
			
		});
		
		boolean ret = false;
		
		try {
			lines.add(0, ATTR_NAME_INSTANCE +";"+ ATTR_NAME_EXECUTION_DAY +";"+ ATTR_NAME_EXECUTION_HOUR +";"+ ATTR_NAME_EXECUTION_TIME);
			FileUtils.writeLines(file, lines);
			ret = true;
		} catch (IOException e) {
			log.error("Failed to save file "+ file.getAbsolutePath() +": "+ e.getMessage());
		}
		
		return ret;
		
	}
	
	public void clearDatabase() {
		
		try {
			p.deleteCaseBase(CASEBASE_DATABASE_NAME);
			instancesCount = 0;
			database = p.createDefaultCB(CASEBASE_DATABASE_NAME);
		} catch (Exception e) {
			log.error("Error when trying to clear database: "+ e.getMessage());
		}

	}
	
	public List<Pair<Instance, Similarity>> query(Map<String, Object> attrMap) throws ParseException {
		
		Retrieval r = new Retrieval(concept, database);
		
		Instance query = r.getQueryInstance();
		
		attrMap.entrySet().stream().forEach(x -> {
			try {
				query.addAttribute(x.getKey(), x.getValue());
			} catch (ParseException e) {
				log.error("Error when add attribute "+ x.getKey() +": "+ e.getMessage());
			}
		});
		
//		System.out.println("Adding "+ executionDayAttr.getName() +": "+ query.addAttribute(executionDayAttr, 1));
//		System.out.println("Adding "+ executionHourAttr.getName() +": "+ query.addAttribute(executionHourAttr, 10));
//		System.out.println("Adding "+ instanceAttr.getName() +": "+ query.addAttribute(instanceAttr, new Attribute() {
//			
//			@Override
//			public String getValueAsString() {
//				return Project.UNDEFINED_SPECIAL_ATTRIBUTE;
//			}
//		}));

		r.start();
		
		return r.getResult();

	}
	
	public static final Map<String, Object> createEmptyAttributeMap() {
		return new HashMap<String, Object>();
	}
	
	public DefaultCaseBase getDatabase() {
		return database;
	}
	
	
	public void importCsvInstances(File file) {
		if (!file.exists()) {
			log.error("File "+ file.getAbsolutePath() +" does not exists.");
		}
		
		CSVImporter imp = new CSVImporter(file.getAbsolutePath(), concept);
		imp.readData();
		imp.checkData();
		imp.addMissingValues();
		imp.addMissingDescriptions();
		imp.doImport();
		
		System.out.print("Importing ");
		while (imp.isImporting()){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.print(".");
		}
		
	}
	
	
	

}