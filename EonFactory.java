package com.medcisive.apm;

//import com.medcisive.apm.hf.DataSourceHFPM;
import com.medcisive.apm.auto.DataSourceAutoPM;
import com.medcisive.utility.PropertiesUtility;
import com.medcisive.utility.LogUtility;
//import edu.stanford.smi.eon.siteCustomization.*;
//import edu.stanford.smi.eon.execEngine.*;
import edu.stanford.smi.eon.execEngine.IDataSource;
import edu.stanford.smi.eon.execEngine.IDataSourceFactory;
import edu.stanford.smi.eon.execEngine.IEON;
import edu.stanford.smi.eon.execEngine.IEonFactory;
import edu.stanford.smi.eon.siteCustomization.BMIRDataSourceFactory;
import edu.stanford.smi.eon.siteCustomization.BMIREonFactory;
import edu.stanford.smi.eon.kbhandler.KBHandler;
import java.util.*;
import java.text.SimpleDateFormat;
import com.medcisive.utility.UtilityFramework;

/**
 *
 * @author vhapalchambj
 */
public class EonFactory {

    private static EonFactory _instance;
    private PropertiesUtility _properties;
    public static SimpleDateFormat DateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    
    private HashMap<String,IEON> _eonMap;  // This contains a map of EON instances;
                                           // Each entry consists: 
                                           //   ID(String), for particular clinical area
                                           //   IEON, instance of EON for particular clinical area
    
    private HashMap<String, String> _guidelineIDMap; //This contains a map of the guidelineID's
                                                     //Each entry consists of:
                                                     // ID(string), for particular clinical area
                                                     // guidelineID(string) for that clinical area
    
    public String _strSessionTime = null;
    
    public enum Type { 
        BMIR
    }

    private EonFactory() {
    }

    public static EonFactory init(String propertyName) {
        _instance = new EonFactory();
        _instance._properties = UtilityFramework._properties;        
        /*
        try {
            _instance._properties = PropertiesUtility.get(propertyName);
        } catch (java.lang.Exception e) {
            System.err.println(e);
        }
        */
        return _instance;
    }
    
    public HashMap<String, String> getGuidelineIDMap() {
       return _guidelineIDMap;
    }
    
    public boolean createEonMap() {
       // Obtain project paths
       String HTNProjPath = _properties.getProperty("HTN_PROJECT_PATH");
       String DMProjPath = _properties.getProperty("DM_PROJECT_PATH");
       String LipidProjPath = _properties.getProperty("Lipid_PROJECT_PATH");
       String CKDProjPath = _properties.getProperty("CKD_PROJECT_PATH");
       String HFProjPath = _properties.getProperty("HF_PROJECT_PATH");
       LogUtility.warn(" DM_PROJECT_PATH: " + DMProjPath);
       LogUtility.warn(" Lipid_PROJECT_PATH: " + LipidProjPath);
       LogUtility.warn(" HTN_PROJECT_PATH: " + HTNProjPath);
       LogUtility.warn(" CKD_PROJECT_PATH: " + CKDProjPath);
       LogUtility.warn(" HF_PROJECT_PATH: " + HFProjPath);
       
       
       IDataSource aDS_HTN = new DataSourceAutoPM();
       IDataSource aDS_DM = new DataSourceAutoPM();
       IDataSource aDS_HF = new DataSourceAutoPM();
       IDataSource aDS_Lipid = new DataSourceAutoPM();
       
       String projPath = null;
       IEonFactory aEonFactory = new BMIREonFactory();
       _eonMap = new HashMap();
       IEON aEON = null;
       try {
          projPath = HTNProjPath;
          aEON = aEonFactory.createEON(projPath, aDS_HTN);
          _eonMap.put(WorkflowManager.HTN_ID, aEON);
          LogUtility.warn(" EonMap loaded HTN instance");
          
          projPath = DMProjPath;
          aEON = aEonFactory.createEON(projPath, aDS_DM);
          _eonMap.put(WorkflowManager.DM_ID, aEON);
          LogUtility.warn(" EonMap loaded DM instance");
          
          projPath = LipidProjPath;
          aEON = aEonFactory.createEON(projPath, aDS_Lipid);
          _eonMap.put(WorkflowManager.Lipid_ID, aEON);
          LogUtility.warn(" EonMap loaded Lipid instance");           
          
          projPath = HFProjPath;
          aEON = aEonFactory.createEON(projPath, aDS_HF);
          _eonMap.put(WorkflowManager.HF_ID, aEON);
          LogUtility.warn(" EonMap loaded HF instance"); 
          
          
          
       } catch (Exception e) {
          LogUtility.error("EON create failed using path: " + projPath );
          e.printStackTrace();
          return false;
       }
       createGuidelineIDMap();
       determineSessionTime();
       return true;
    }
    
    private void createGuidelineIDMap() {
       //
       String HTNglID = _properties.getProperty("HTN_GUIDELINE_ID");
       String DMglID = _properties.getProperty("DM_GUIDELINE_ID");
       String LipidglID = _properties.getProperty("Lipid_GUIDELINE_ID");
       String CKDglID = _properties.getProperty("CKD_GUIDELINE_ID");
       String HFglID = _properties.getProperty("HF_GUIDELINE_ID");
       
       _guidelineIDMap = new HashMap();       
       _guidelineIDMap.put(WorkflowManager.HTN_ID, HTNglID );
       _guidelineIDMap.put(WorkflowManager.DM_ID, DMglID );     
       _guidelineIDMap.put(WorkflowManager.CKD_ID, CKDglID );      
       _guidelineIDMap.put(WorkflowManager.Lipid_ID, LipidglID );  
       _guidelineIDMap.put(WorkflowManager.HF_ID, HFglID );       
       
    }
    
    private void determineSessionTime() {
       String strSessTime = _properties.getProperty("SESSION_TIME");
       if (strSessTime != null && strSessTime.length() > 0 ) {
           _strSessionTime = strSessTime;
       }
       else {
           Calendar now = Calendar.getInstance();
           Date today = now.getTime();
           _strSessionTime = DateFormatter.format(today);           
       }           
    }
    
    /*
     * This method returns the Eon instance for the 
     * clinical domain specified by the input ID.
     * The input ID must be one found in the list in 
     * Workflow Manager (HTN_ID,DM_ID, CKD_ID, etc.)
     */
    public IEON getEonEngine(String clinicalDomainID) {
      if (_eonMap == null || _eonMap.size() <= 0 )  {
         LogUtility.error(" getEonEngine called failed because _eonMap is not set up. ");
         return null;
      }
      IEON anEon= null; 
      anEon = _eonMap.get(clinicalDomainID);
        
      return anEon;
        
    }
    
    /*
     * This method returns the Eon instance for the 
     * clinical domain specified by the input ID.
     * The input ID must be one found in the list in 
     * Workflow Manager (HTN_ID,DM_ID, CKD_ID, etc.)
     */
    public String getGuidelineID(String clinicalDomainID) {
      if (_guidelineIDMap == null || _guidelineIDMap.size() <= 0 )  {
         LogUtility.error(" getGuidelineID called failed because _guidelineIDMap is not set up. ");
         return null;
      }
      return _guidelineIDMap.get(clinicalDomainID);        
    }    
    

    public static EonFactory get() {
        return _instance;
    }

    public IEON create(Type type) throws java.lang.Exception {
        IEON result = null;
        IDataSource ds = null;
        try {
            result = createEON(type);
        } catch (java.lang.Exception e) {
            throw new Exception("EonFactory.createEON: " + e);
        }
        try {
            ds = createDataSource(type);
        } catch (java.lang.Exception e) {
            throw new Exception("EonFactory.createDataSource: " + e);
        }
        if (result == null) {
            return null;
        }
        result.setDataSource(ds);
        return result;
    }
    
    

    public IDataSource createDataSource(Type type) throws java.lang.Exception {
        switch (type) {
            case BMIR:
                //return new DataSourceHFPM();
            default:
                break;
        }
        return null;
    }

    public IEON createEON(Type type) throws Exception {
        String path = _properties.getProperty("PROTEGE_PATH");
        IEON aEON = null;
        switch (type) {
            case BMIR:
                // use BMIR factory method instead
                IEonFactory aEonFactory = new BMIREonFactory();
                try {
                    aEON = aEonFactory.createEON(path);
                } catch (Exception e) {
                    LogUtility.error("EON create failed using path: " + path);
                    e.printStackTrace();
                }
                return aEON;
            default:
                break;
        }
        return null;
    }
}
