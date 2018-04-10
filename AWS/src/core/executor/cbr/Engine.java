package core.executor.cbr;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.dfki.mycbr.core.Project;
import de.dfki.mycbr.core.model.Concept;
import de.dfki.mycbr.io.CSVImporter;

public class Engine {

	private final Logger log = LogManager.getLogger(getClass());

	private static String PROJECT_FILENAME = "project.prj";
	public static String CONCEPT_NAME = "TaskExecution";
	public static String CASEBASE_NAME = "CBRDatabase";
	
	private static String CURRENT_DIR = System.getProperty("user.dir");
	private static String DATA_PATH = CURRENT_DIR + File.separatorChar + "cbr";
	private static String PROJECT_FILE_PATH = DATA_PATH + File.separatorChar + PROJECT_FILENAME;
	private static String CSV_DATA_FILE_PATH = DATA_PATH + File.separatorChar +"cases.txt";
	

	public static String getCaseBase() {
		return CASEBASE_NAME;
	}

	public static void setCasebase(String casebase) {
		Engine.CASEBASE_NAME = casebase;
	}

	public static String getConceptName() {
		return CONCEPT_NAME;
	}

	public static void setConceptName(String conceptName) {
		Engine.CONCEPT_NAME = conceptName;
	}

	public static String getCsv() {
		return CSV_DATA_FILE_PATH;
	}

	public static void setCsv(String csv) {
		Engine.CSV_DATA_FILE_PATH = csv;
	}

	public Project createProjectFromPRJ() throws Exception {
		return createProjectFromPRJ(PROJECT_FILE_PATH);
	}

	public Project createProjectFromPRJ(String filePath) throws Exception {
		
		log.debug("Creating CBR Project from file : " + filePath);
		
		Project project = null;
		
		try {
			project = new Project(filePath);
			project.createDefaultCB(CASEBASE_NAME);
			
			while (project.isImporting()) {
				Thread.sleep(1000);
			}
		} catch (Exception ex) {
			throw ex;
		}
		
		
		return project;
	}
	
	public Project createFullCBRProject() throws Exception {
		return createFullCBRProject(PROJECT_FILE_PATH, CSV_DATA_FILE_PATH);		
	}
	
	public Project createFullCBRProject(String projectPath, String dataPath) throws Exception {

		Project project = null;
		try {
			
			project = createProjectFromPRJ(projectPath);
			
			Concept concept = project.createTopConcept(CONCEPT_NAME);
			
			CSVImporter csvImporter = new CSVImporter(dataPath, concept);
			csvImporter.setCaseBase(project.getCB(CASEBASE_NAME));
			
			csvImporter.setSeparator(";");
			
			// prepare for import
			csvImporter.readData();
			csvImporter.checkData();
			csvImporter.addMissingValues();
			csvImporter.addMissingDescriptions();
			
			// do the import of the instances
			csvImporter.doImport();
			
			// wait until the import is done
			while (csvImporter.isImporting()) {
				Thread.sleep(1000);
			}
			log.info("Imported "+ csvImporter.getTotalNumberOfCases() +" cases into "+ CASEBASE_NAME);
		} catch (Exception e) {
			throw e;
		}
		return project;
	}

}