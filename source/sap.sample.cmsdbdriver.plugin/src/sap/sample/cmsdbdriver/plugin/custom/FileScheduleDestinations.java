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

	private static final String OUTPUTFILEPATH = "OutputFilePath";
	
	FileWriter fw;
	final private PluginBase pluginBase;
	
	final private Map<String, UnvTableFieldDef> columns = new HashMap<String, UnvTableFieldDef>();

	/**
	 * Define the list of Fields for the virtual table
	 */
	public FileScheduleDestinations(IResultPlugin plugin) {
		super(plugin);
		columns.put(OUTPUTFILEPATH, new UnvTableFieldDef(OUTPUTFILEPATH, Types.VARCHAR));
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
		/*
		try {
			fw = new FileWriter("C:\\Temp\\debug.txt");
		} catch (IOException e2) {
			e2.printStackTrace();
		}
*/
	}

	@Override
	public void setValues(int id) throws CMSDBDriverException {
			
		//Process the CMS query
		String OutputFile =	processQuery(id);
		
				
		setObjectProperty(TABLE_NAME + "." + FileScheduleDestinations.OUTPUTFILEPATH,
				String.class.getName(), OutputFile);
		addRow(id);
		
			
	}

	@SuppressWarnings("unused")
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
		//Initialize Return Variable
		String OutputFile = "";
		
		
		IInfoObjects infoObjects = pluginBase.getConnection().queryCMS("SELECT SI_NAME, SI_ID, SI_SCHEDULEINFO FROM CI_INFOOBJECTS where si_id = " + id);
		
		if (infoObjects == null) {
			return OutputFile;
		}
		
		int recordCount = infoObjects.size();
		
		if (recordCount == 0) {
			return OutputFile;
		}
		
		Iterator infoObjectsIter = infoObjects.iterator();
		
		while (infoObjectsIter.hasNext()) {
			
			IInfoObject infoObject = (IInfoObject)infoObjectsIter.next();
			
			ISchedulingInfo sInfo = infoObject.getSchedulingInfo();
			
			IDestinations dests = sInfo.getDestinations();
			
			String pluginType = "CrystalEnterprise.DiskUnmanaged";
			Iterator destIter = dests.iterator();
			
			IDestination dest=null;
			while (destIter.hasNext()) {
				
				dest = (IDestination) destIter.next();
				if (pluginType.equals(dest.getName()))
				{
					break;
				}

			}
			
			if (dest == null)
			{
				return OutputFile;
			}
			
			if (dest.getName().equals(pluginType)) {
				IProperties properties = dest.properties();
				
				IProperty scheduleOptions = properties.getProperty(CePropertyID.SI_DEST_SCHEDULEOPTIONS);
				if ( scheduleOptions== null)
				{
					break;
				}
				
				IProperties scheduleOptionsProperties=(IProperties)properties.getProperty(CePropertyID.SI_DEST_SCHEDULEOPTIONS).getValue();
				if (scheduleOptionsProperties == null)
				{
					break;					
				}
				//Retrieve SI_OUTPUT_FILES property
				IProperty outputFilesProperty = scheduleOptionsProperties.getProperty("SI_OUTPUT_FILES");
				if (outputFilesProperty == null)
				{
					break;
				}
				
				//Retrieve Properties of SI_OUTPUT_FILES
				IProperties outputFilesProperties = (IProperties)scheduleOptionsProperties.getProperty("SI_OUTPUT_FILES").getValue();
				if (outputFilesProperties == null)
				{
					break;					
				}
				//Retrieve Filename
				IProperty filenameProperty = outputFilesProperties.getProperty("1");
				if (filenameProperty == null)
				{
					break;
				}
				else
				{
					OutputFile = filenameProperty.getValue().toString();
				}
				
			}
			
			
		}

		return OutputFile;
	}
}	

