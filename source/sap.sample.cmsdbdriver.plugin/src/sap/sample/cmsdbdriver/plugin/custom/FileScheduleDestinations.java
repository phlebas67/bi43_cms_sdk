package sap.sample.cmsdbdriver.plugin.custom;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import com.crystaldecisions.sdk.occa.infostore.CePropertyID;
import com.crystaldecisions.sdk.occa.infostore.IDestination;
import com.crystaldecisions.sdk.occa.infostore.IDestinations;
import com.crystaldecisions.sdk.occa.infostore.IInfoObject;
import com.crystaldecisions.sdk.occa.infostore.IInfoObjects;
import com.crystaldecisions.sdk.occa.infostore.ISchedulingInfo;
import com.crystaldecisions.sdk.properties.IProperties;
import com.crystaldecisions.sdk.properties.IProperty;
import com.sap.connectivity.cs.java.drivers.cms.CMSDBDriverException;
import com.sap.connectivity.cs.java.drivers.cms.api.IQueryElement;
import com.sap.connectivity.cs.java.drivers.sdk.datafoundation.IUnvTable;
import com.sap.connectivity.cs.java.drivers.sdk.datafoundation.UnvTableFieldDef;

import sap.sample.cmsdbdriver.plugin.core.IResultPlugin;
import sap.sample.cmsdbdriver.plugin.core.IResultTable;
import sap.sample.cmsdbdriver.plugin.core.PluginBase;

public class FileScheduleDestinations extends IResultTable implements IUnvTable {

	private static final String TABLE_NAME = "FileScheduleDestination";

	private static final String NO = "no";
	private static final String TEXT = "text";
	
	FileWriter fw;
	final private PluginBase pluginBase;
	
	final private Map<String, UnvTableFieldDef> columns = new HashMap<String, UnvTableFieldDef>();

	/**
	 * Define the list of Fields for the virtual table
	 */
	public FileScheduleDestinations(IResultPlugin plugin) {
		super(plugin);
		columns.put(NO, new UnvTableFieldDef(NO, Types.INTEGER));
		columns.put(TEXT, new UnvTableFieldDef(TEXT, Types.VARCHAR));
		pluginBase = (PluginBase)plugin;
	}
	
	/**
	 * Returns the name of the virtual table
	 * @return virtual table name
	 */
	@Override
	public String getName() {
		return TABLE_NAME;
	}

	@Override
	/**
	 * Returns the fields defined for the virtual table
	 * @return list of table fields
	 */
	public Map<String, UnvTableFieldDef> getTableFields() {
		return this.columns;
	}

	@Override
	/**
	 * use the getQueryElement() or getIds() to prepare data for setValues()
	 */
	public void initialize(IQueryElement queryElement, Set<Integer> ids) {
		// 
		
		try {
			fw = new FileWriter("C:\\Temp\\debug.txt");
		} catch (IOException e2) {
			e2.printStackTrace();
		}

	}

	@Override
	public void setValues(int id) throws CMSDBDriverException {
		writeDebug("In setValues with id: " + id);
		
		//Process the CMS query
		String OutputFile =	processQuery(id);
		
				
		setObjectProperty(TABLE_NAME + "." + FileScheduleDestinations.NO,
				Integer.class.getName(), id);
		setObjectProperty(TABLE_NAME + "." + FileScheduleDestinations.TEXT,
				String.class.getName(), OutputFile);
		addRow(id);
		
		writeDebug("Exiting setValues");	
	}

	private void writeDebug(String debugstr) {
		try {
			fw.write(debugstr);
			fw.write("\r\n");
			fw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@SuppressWarnings("rawtypes")
	private String processQuery(int id){
		//Initialise Return Variable
		String OutputFile = "";
		
		writeDebug("In processQuery()");
		
		writeDebug("About to execute CMS query: SELECT SI_NAME, SI_ID, SI_SCHEDULEINFO FROM CI_INFOOBJECTS where si_id = " + id);
		IInfoObjects infoObjects = pluginBase.getConnection().queryCMS("SELECT SI_NAME, SI_ID, SI_SCHEDULEINFO FROM CI_INFOOBJECTS where si_id = " + id);
		
		if (infoObjects == null) {
			writeDebug("infoObjects query returned null");
			return OutputFile;
		}
		
		int recordCount = infoObjects.size();
		
		if (recordCount == 0) {
			writeDebug("infoObjects query returned 0 records");
			return OutputFile;
		}
		else
			writeDebug("infoObjects query returned " + recordCount + " records");
		
		writeDebug("Retrieving infoObject..");
		Iterator infoObjectsIter = infoObjects.iterator();
		
		while (infoObjectsIter.hasNext()) {
			writeDebug("In infoObject iterator");
			
			IInfoObject infoObject = (IInfoObject)infoObjectsIter.next();
			writeDebug("..infoObject retrieved "+ infoObject.getTitle());
			
			writeDebug("Retrieving SchedulingInfo..");
			ISchedulingInfo sInfo = infoObject.getSchedulingInfo();
			writeDebug("..SchedulingInfo retrieved");
			
			writeDebug("Retrieving Destinations..");
			IDestinations dests = sInfo.getDestinations();
			writeDebug(dests.size() +" Destinations retrieved");
			
			String pluginType = "CrystalEnterprise.DiskUnmanaged";
			Iterator destIter = dests.iterator();
			
			IDestination dest=null;
			while (destIter.hasNext()) {
				writeDebug("In Destinations iterator");
				
				
				dest = (IDestination) destIter.next();
				writeDebug("Destination Name: " + dest.getName());
				if (pluginType.equals(dest.getName()))
				{
					writeDebug("Found a destination with type " +pluginType.toString()+" so breaking out of while loop");
					break;
				}

				writeDebug("Exiting Destinations iterator without finding destination type "+pluginType.toString());
			}
			
			if (dest.getName().equals(pluginType)) {
				writeDebug("In Processing DiskUnManaged destination block");
				writeDebug("About to query properties");
				writeDebug("IProperties properties = dest.properties();");
				IProperties properties = dest.properties();
				
				writeDebug("About to get size of properties");
				writeDebug("Properties size = " + properties.size());
				
				writeDebug("About to run IProperty scheduleOptions = properties.getProperty(CePropertyID.SI_DEST_SCHEDULEOPTIONS);");
				IProperty scheduleOptions = properties.getProperty(CePropertyID.SI_DEST_SCHEDULEOPTIONS);
				if ( scheduleOptions== null)
				{
					writeDebug("No property with name SI_DEST_SCHEDULEOPTIONS exists");
					break;
				}
				
				IProperties scheduleOptionsProperties=(IProperties)properties.getProperty(CePropertyID.SI_DEST_SCHEDULEOPTIONS).getValue();
				if (scheduleOptionsProperties == null)
				{
					writeDebug("Couldn't retrieve properties of SI_DEST_SCHEDULEOPTIONS");
					break;					
				}
				//Retrieve SI_OUTPUT_FILES property
				writeDebug("About to execute IProperty outputFilesProperty = scheduleOptionsProperties.getProperty(CePropertyID.SI_OUTPUT_FILES_PER_DOC);");
				IProperty outputFilesProperty = scheduleOptionsProperties.getProperty("SI_OUTPUT_FILES");
				if (outputFilesProperty == null)
				{
					writeDebug("Couldn't retrieve the SI_OUTPUT_FILES property");
					break;
				}
				
				//Retrieve Properties of SI_OUTPUT_FILES
				writeDebug("About to execute IProperties outputFilesProperties = (IProperties)scheduleOptionsProperties.getProperty(\"SI_OUTPUT_FILES\").getValue();");
				IProperties outputFilesProperties = (IProperties)scheduleOptionsProperties.getProperty("SI_OUTPUT_FILES").getValue();
				if (outputFilesProperties == null)
				{
					writeDebug("Couldn't retrieve properties of SI_OUTPUT_FILES");
					break;					
				}
				//Retrieve Filename
				writeDebug("About to retrieve filename (1) property");
				IProperty filenameProperty = outputFilesProperties.getProperty("1");
				if (filenameProperty == null)
				{
					writeDebug("Couldn't retrieve (1) property");
					break;
				}
				else
				{
					OutputFile = filenameProperty.getValue().toString();
					writeDebug("Filepath = "+OutputFile);
				}
				
			}
			
			
			writeDebug("Exiting infoObject iterator");
		}

		writeDebug("Exiting processQuery()"+"\r\n");
		return OutputFile;
	}
}	

