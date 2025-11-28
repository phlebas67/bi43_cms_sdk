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

import com.crystaldecisions.sdk.plugin.desktop.user.IUser;
import com.crystaldecisions.sdk.plugin.desktop.user.IUserAlias;


public class ComsetExtensions extends IResultTable implements IUnvTable {

	private static final String TABLE_NAME = "ComsetExtensions";
	private static final String SCHEDULEDFILEDESTINATION = "ScheduledFileDestination";
	private static final String EMAILRECIPIENTS = "EmailRecipients";
	private static final String EMAILRECIPIENTS_BCC = "EmailRecipients_BCC";
	private static final String EMAILRECIPIENTS_CC = "EmailRecipients_CC";
	private static final String ISFHSQL = "IsFHSQL?";
	private static final String ALIASENABLED_ENTERPRISE = "secEnterpriseAliasEnabled?";
	private static final String SEC_ENTERPRISE = "secEnterprise";
	private static final String ALIASENABLED_LDAP = "secLDAPAliasEnabled?";
	private static final String SEC_LDAP = "secLDAP";
	private static final String ALIASENABLED_WINAD = "secWinADAliasEnabled?";
	private static final String SEC_WINAD = "secWinAD";

    private final static boolean DEBUGMODE=false;
    
	FileWriter fw;
	final private PluginBase pluginBase;
	
	final private Map<String, UnvTableFieldDef> columns = new HashMap<String, UnvTableFieldDef>();

	/**
	 * Define the list of Fields for the virtual table
	 */
	public ComsetExtensions(IResultPlugin plugin) {
		super(plugin);
		columns.put(SCHEDULEDFILEDESTINATION, new UnvTableFieldDef(SCHEDULEDFILEDESTINATION, Types.VARCHAR));
		columns.put(EMAILRECIPIENTS, new UnvTableFieldDef(EMAILRECIPIENTS, Types.VARCHAR));
		columns.put(EMAILRECIPIENTS_BCC, new UnvTableFieldDef(EMAILRECIPIENTS_BCC, Types.VARCHAR));
		columns.put(EMAILRECIPIENTS_CC, new UnvTableFieldDef(EMAILRECIPIENTS_CC, Types.VARCHAR));
		columns.put(ISFHSQL, new UnvTableFieldDef(ISFHSQL, Types.VARCHAR));
		columns.put(ALIASENABLED_ENTERPRISE, new UnvTableFieldDef(ALIASENABLED_ENTERPRISE, Types.VARCHAR));
		columns.put(ALIASENABLED_LDAP, new UnvTableFieldDef(ALIASENABLED_LDAP, Types.VARCHAR));
		columns.put(ALIASENABLED_WINAD, new UnvTableFieldDef(ALIASENABLED_WINAD, Types.VARCHAR));
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
		
		if (DEBUGMODE)
		{
			try {
				fw = new FileWriter("C:\\Temp\\comsetextensions_debug.txt");
			} catch (IOException e2) {
				e2.printStackTrace();
			}
		}
	}

	@Override
	public void setValues(int id) throws CMSDBDriverException {
		writeDebug("In setValues with id: " + id);
		
		//Retrieve scheduled file destinations if they exist..
		String OutputFile =	retrieveScheduledFileDestinations(id);
		
		setObjectProperty(TABLE_NAME + "." + ComsetExtensions.SCHEDULEDFILEDESTINATION,
				String.class.getName(), OutputFile);
		
		//Retrieve Email Recipients if they exist..
		String[] EmailRecipients = retrieveScheduledEmailDestinations(id);
		
		setObjectProperty(TABLE_NAME + "." + ComsetExtensions.EMAILRECIPIENTS,
				String.class.getName(), EmailRecipients[0]);

		setObjectProperty(TABLE_NAME + "." + ComsetExtensions.EMAILRECIPIENTS_BCC,
				String.class.getName(), EmailRecipients[1]);
		
		setObjectProperty(TABLE_NAME + "." + ComsetExtensions.EMAILRECIPIENTS_CC,
				String.class.getName(), EmailRecipients[2]);
		
		//Check if a FHSQL data-provider
		if (isFHSQL(id))
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ISFHSQL,String.class.getName(), "True");			
		else
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ISFHSQL,String.class.getName(), "False");

		//Retrieve secEnterpriseAliasrEnabled flag
		if (isUserEnabled(id,SEC_ENTERPRISE))
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_ENTERPRISE,String.class.getName(), "True");			
		else
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_ENTERPRISE,String.class.getName(), "False");
		
		if (isUserEnabled(id,SEC_LDAP))
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_LDAP,String.class.getName(), "True");			
		else
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_LDAP,String.class.getName(), "False");
		
		if (isUserEnabled(id,SEC_WINAD))
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_WINAD,String.class.getName(), "True");			
		else
			setObjectProperty(TABLE_NAME + "." + ComsetExtensions.ALIASENABLED_WINAD,String.class.getName(), "False");

		//Write the row
		addRow(id);
		
		writeDebug("Exiting setValues");	
	}

	private void writeDebug(String debugstr) {
		if (DEBUGMODE)
		{
			try {
				fw.write(debugstr);
				fw.write("\r\n");
				fw.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private String retrieveScheduledFileDestinations(int id){
		//Initialize Return Variable
		String OutputFile = "";
		
		writeDebug("In retrieveScheduledFileDestinations()");
		
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
			try {
				if (dest.getName().equals(pluginType)) {
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
				}
			}
			catch (Exception e) {
				writeDebug("In Catch with error: "+e+"\r\n");
				return OutputFile;
			}

			
			
			writeDebug("Exiting infoObject iterator");
		}

		writeDebug("Exiting retrieveScheduledFileDestinations()"+"\r\n");
		return OutputFile;
	}
	
	@SuppressWarnings("rawtypes")
	private String[] retrieveScheduledEmailDestinations(int id){
		//Initialize Return Variable
		String[] EmailRecipients = {"","",""};
		
		writeDebug("In retrieveScheduledEmailDestinations()");
		
		writeDebug("About to execute CMS query: SELECT SI_NAME, SI_ID, SI_SCHEDULEINFO FROM CI_INFOOBJECTS where si_id = " + id);
		IInfoObjects infoObjects = pluginBase.getConnection().queryCMS("SELECT SI_NAME, SI_ID, SI_SCHEDULEINFO FROM CI_INFOOBJECTS where si_id = " + id);
		
		if (infoObjects == null) {
			writeDebug("infoObjects query returned null");
			return EmailRecipients;
		}
		
		int recordCount = infoObjects.size();
		
		if (recordCount == 0) {
			writeDebug("infoObjects query returned 0 records");
			return EmailRecipients;
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
			
			String pluginType = "CrystalEnterprise.Smtp";
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
			try {
				if (dest.getName().equals(pluginType)) {
					if (dest.getName().equals(pluginType)) {
						writeDebug("In Processing SMTP destination block");
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
						
						//Retrieve SI_MAIL_ADDRESSES property
						writeDebug("About to execute IProperty mailAddresses = scheduleOptionsProperties.getProperty(SI_MAIL_ADDRESSES);");
						IProperty mailAddresses = scheduleOptionsProperties.getProperty("SI_MAIL_ADDRESSES");
						if (mailAddresses == null)
						{
							writeDebug("Couldn't retrieve the SI_MAIL_ADDRESSES property");
							break;
						}
						//Retrieve Properties of SI_MAIL_ADDRESSES
						writeDebug("About to execute IProperties mailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty(\"SI_MAIL_ADDRESSES\").getValue();");
						IProperties mailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty("SI_MAIL_ADDRESSES").getValue();
						if (mailAddressesProperties == null)
						{
							writeDebug("Couldn't retrieve properties of SI_MAIL_ADDRESSES");
							break;					
						}
						
						//Determine the number of email addresses retrieved
						String numberofEmails = mailAddressesProperties.getProperty("SI_TOTAL").getValue().toString();
						writeDebug("Number of emails retrieved = "+numberofEmails);

						Integer EmailIterator = (Integer) mailAddressesProperties.getProperty("SI_TOTAL").getValue();
						for (int i = 1; i <= EmailIterator; i++) {
							String email = "";
							email = mailAddressesProperties.getProperty(""+i).getValue().toString();
							writeDebug("Email address " + i + " = "+email);
							if (i==1)
								EmailRecipients[0] = email;
							else
								EmailRecipients[0] = EmailRecipients[0] + ";" + email;
						}
						//Retrieve SI_MAIL_BCC property
						writeDebug("About to execute IProperty bccmailAddresses = scheduleOptionsProperties.getProperty(SI_MAIL_BCC);");
						IProperty bccmailAddresses = scheduleOptionsProperties.getProperty("SI_MAIL_BCC");
						if (bccmailAddresses == null)
						{
							writeDebug("Couldn't retrieve the SI_MAIL_BCC property");
							break;
						}
						//Retrieve Properties of SI_MAIL_ADDRESSES
						writeDebug("About to execute IProperties bccmailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty(\"SI_MAIL_BCC\").getValue();");
						IProperties bccmailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty("SI_MAIL_BCC").getValue();
						if (bccmailAddressesProperties == null)
						{
							writeDebug("Couldn't retrieve properties of SI_MAIL_BCC");
							break;					
						}
						
						//Determine the number of email addresses retrieved
						String numberofbccEmails = bccmailAddressesProperties.getProperty("SI_TOTAL").getValue().toString();
						writeDebug("Number of bcc emails retrieved = "+numberofbccEmails);

						Integer bccEmailIterator = (Integer) bccmailAddressesProperties.getProperty("SI_TOTAL").getValue();
						for (int i = 1; i <= bccEmailIterator; i++) {
							String email = "";
							email = bccmailAddressesProperties.getProperty(""+i).getValue().toString();
							writeDebug("BCC Email address " + i + " = "+email);
							if (i==1)
								EmailRecipients[1] = email;
							else
								EmailRecipients[1] = EmailRecipients[1] + ";" + email;
						}

						
						//Retrieve SI_MAIL_CC property
						writeDebug("About to execute IProperty bccmailAddresses = scheduleOptionsProperties.getProperty(SI_MAIL_CC);");
						IProperty ccmailAddresses = scheduleOptionsProperties.getProperty("SI_MAIL_CC");
						if (ccmailAddresses == null)
						{
							writeDebug("Couldn't retrieve the SI_MAIL_CC property");
							break;
						}
						//Retrieve Properties of SI_MAIL_ADDRESSES
						writeDebug("About to execute IProperties bccmailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty(\"SI_MAIL_CC\").getValue();");
						IProperties ccmailAddressesProperties = (IProperties)scheduleOptionsProperties.getProperty("SI_MAIL_CC").getValue();
						if (ccmailAddressesProperties == null)
						{
							writeDebug("Couldn't retrieve properties of SI_MAIL_CC");
							break;					
						}
						
						//Determine the number of email addresses retrieved
						String numberofccEmails = ccmailAddressesProperties.getProperty("SI_TOTAL").getValue().toString();
						writeDebug("Number of cc emails retrieved = "+numberofccEmails);

						Integer ccEmailIterator = (Integer) ccmailAddressesProperties.getProperty("SI_TOTAL").getValue();
						for (int i = 1; i <= ccEmailIterator; i++) {
							String email = "";
							email = ccmailAddressesProperties.getProperty(""+i).getValue().toString();
							writeDebug("CC Email address " + i + " = "+email);
							if (i==1)
								EmailRecipients[2] = email;
							else
								EmailRecipients[2] = EmailRecipients[2] + ";" + email;
						}

					}
				}
			}
			catch (Exception e) {
				writeDebug("In Catch with error: "+e+"\r\n");
				return EmailRecipients;
			}
			writeDebug("Exiting infoObject iterator");
		}
		writeDebug("Exiting retrieveScheduledEmailDestinations()"+"\r\n");
		return EmailRecipients;
	}
	
	private Boolean isFHSQL(int id){
		
		writeDebug("In isFHSQL()");
		
		writeDebug("About to execute CMS query: select SI_ID from CI_INFOOBJECTS where SI_KIND = 'Webi' and SI_FHSQL_RELATIONAL_CONNECTION.SI_TOTAL > 0 and SI_ID = " + id);
		IInfoObjects infoObjects = pluginBase.getConnection().queryCMS("SELECT SI_ID from CI_INFOOBJECTS where SI_KIND = 'Webi' and SI_FHSQL_RELATIONAL_CONNECTION.SI_TOTAL > 0 and SI_ID = " + id);
		
		if (infoObjects == null) {
			writeDebug("infoObjects query returned null");
			return false;
		}
		
		int recordCount = infoObjects.size();
		
		if (recordCount == 0) {
			writeDebug("infoObjects query returned 0 records");
			return false;
		}
		
		writeDebug("infoObjects query returned " + recordCount + " records");
		return true;
	}
	
	private Boolean isUserEnabled(int id, String secAliasType){
		
		Boolean aliasEnabled = false;

		
		writeDebug("In isUserEnabled()");
		
		writeDebug("About to execute CMS query: SELECT SI_ID, SI_NAME, SI_Aliases From CI_SYSTEMOBJECTS Where SI_KIND='user' and SI_ID = " + id);
		IInfoObjects userCollection = pluginBase.getConnection().queryCMS("SELECT SI_ID, SI_NAME, SI_Aliases From CI_SYSTEMOBJECTS Where SI_KIND='user' and SI_ID = " + id);
		
		if (userCollection == null) {
			writeDebug("userCollection query returned null");
			return false;
		}
		
		int recordCount = userCollection.size();
		
		if (recordCount == 0) {
			writeDebug("userCollection query returned 0 records");
			return false;
		}
		
		writeDebug("userCollection query returned " + recordCount + " records");
		
		writeDebug("About to instantiate userIterator");
		@SuppressWarnings("rawtypes")
		Iterator userIterator = userCollection.iterator();
		
		//Iterate through list of users
		while (userIterator.hasNext()) {
			writeDebug("About to instantiate user object");
			IUser user = (IUser) userIterator.next();
			
			// Retrieve UserName
			String username = user.getTitle();
			writeDebug("Retrieved information for userID " + id + ", Name= "+username);
			
			//Retrieve list of aliases
			writeDebug("Retrieving user aliases");
			@SuppressWarnings("rawtypes")
			Iterator aliasIterator = user.getAliases().iterator();
			Boolean specifiedAliasTypeFound = false;
			
			while (aliasIterator.hasNext()) {
				writeDebug("Retrieving alias");
				IUserAlias userAlias = (IUserAlias) aliasIterator.next();
				
				//Retrieve Alias Name
				writeDebug("Retrieving Alias Details");
				String aliasName=userAlias.getName();
				Integer aliasType=userAlias.getType();
				Boolean aliasDisabled=userAlias.isDisabled();
				writeDebug("Alias Name:"+aliasName);
				writeDebug("Alias Type:"+aliasType);
				writeDebug("Alias Disabled?:"+aliasDisabled);
				
				// Test to see if the retrieved alias is the required type
				writeDebug("Testing Alias Type");
				
				if (secAliasType == SEC_ENTERPRISE) {
					writeDebug("Testing for "+SEC_ENTERPRISE);
					if (aliasType == IUserAlias.ENTERPRISE) {
						specifiedAliasTypeFound = true;
						writeDebug(SEC_ENTERPRISE+" alias found");
					}
				}
				else if (secAliasType == SEC_LDAP) {
					writeDebug("Testing for "+SEC_LDAP);
					if (aliasType == IUserAlias.THIRD_PARTY && aliasName.contains(SEC_LDAP)) {
						specifiedAliasTypeFound = true;
						writeDebug(SEC_LDAP+" alias found");
					}
				}
				else if (secAliasType == SEC_WINAD) {
					writeDebug("Testing for "+SEC_WINAD);
					if (aliasType == IUserAlias.THIRD_PARTY && aliasName.contains(SEC_WINAD)) {
						specifiedAliasTypeFound = true;
						writeDebug(SEC_WINAD+" alias found");
					}
				}
				
				//If Alias has been found, test to see if the Alias is enabled
				if (specifiedAliasTypeFound) {
					writeDebug("Testing to see if alias is enabled");
					if (aliasDisabled) {
						writeDebug("Alias is disabled");
						aliasEnabled = false;
					}
					else {
						writeDebug("Alias is enabled");
						aliasEnabled = true;
					}
					writeDebug("Exiting function");
					return aliasEnabled;
				}
				
			}
				
		}
		
		return aliasEnabled;
	}
}	

