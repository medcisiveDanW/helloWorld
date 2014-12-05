package com.medcisive.apm.auto;


import com.medcisive.utility.LogUtility;
import com.medcisive.utility.PropertiesUtility;
import com.medcisive.utility.Timer;
//import com.medcisive.utility.sql.DatabaseController;
import com.medcisive.utility.sql2.SQLObject;
import com.medcisive.utility.sql2.DBC;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.*;
import java.util.*;

/**
 *
 * @author vhapalwangd
 */
public class AutoPMDBC extends com.medcisive.apm.ApmDBC {

    public static String COMMA = ",";
    public static int BUFFER_SIZE = 500;
    private static SimpleDateFormat DateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private String m_strSessionTime;   // in the format 'yyyy-mm-dd',  e.g. '2013-12-05'

    public AutoPMDBC() { 
        //super(args);
        //LogUtility.warn(" AutoPMDBC constructor start arg 0: " + args[0]);        
        determineSessionTime();
    }

    private void determineSessionTime() {
        String strSessTime = _properties.getProperty("SESSION_TIME");
        if (strSessTime != null && strSessTime.length() > 0) {
            m_strSessionTime = strSessTime;
        } else {
            Calendar now = Calendar.getInstance();
            Date today = now.getTime();
            m_strSessionTime = DateFormatter.format(today);
        }
        LogUtility.debug("DBC m_strSessionTime " + m_strSessionTime);
    }
    /*
     * In identifyPatients, the table GlobalProviderReport is queried.
     * Those patients who do not meet performance measures are identified. The names
     * of these patients is saved into AthenaDemographics, and for each, the guidelinesToDoMap
     * saves which guidelines need to be done according to the rules in GuidelinesToDoMapper.
     */    
   public int identifyPatients() {
        if (!_properties.runThisFunction()) {
            return -1;
        }
        Timer t = Timer.start();
        _dest.update("DELETE FROM dbo.AthenaDemographics");
        String q =
                " select pt.sta3n, \n"
                + "        pt.patientSID, \n"
                + "        pt.patientSSN, \n"
                + "        pt.patientIEN,  \n"
                + "        pt.patientName,  \n"
                + "        pt.DateOfBirth,  \n"
                + "        pt.race,  \n"
                + "        pt.gender,  \n"
                + "        gpr.BP_PerformanceMeasureKey, \n"
                + "        gpr.DMStatus,  \n"
                + "        gpr.HTNStatus,  \n"
                + "        gpr.A1cLessThan9_PerformanceMeasureKey,  \n"
                + "        gpr.DM_Nephropathy_Key,  \n"
                + "        gpr.LDLLessThan100_PerformanceMeasureKey,  \n"
                + "        gpr.IHDStatus, \n"
                + "        gpr.HF_PerformanceMeasureKey \n"                
                + "FROM CPMReports.GlobalProviderReport gpr, \n"
                + "     SPatient.SPatient pt \n"
                + "where gpr.sta3n = pt.sta3n \n"
                + "  and gpr.patientSID = pt.patientSID \n"
                + " order by pt.patientSID ";       
                
        _src.query(q, new SQLObject(){
            
           String insertRoot =
                "insert into AthenaDemographics "
                + "  (sta3n,patientSID,ssn,patientDFN,name,DOB,race,sex,guidelinesToDoMap)";
           String unionAll = " union all ";           
           
           StringBuffer insStat = new StringBuffer(insertRoot);
           int result = 0;
           int prevSID = -1;            
           int countRows = 0;
           
           public void row(ResultSet rs) throws SQLException {
               PtPerformanceMeasure aPM = new PtPerformanceMeasure(rs);
               if (aPM.noneToDo()) {
                 return;
               }
               if (prevSID != aPM.patientSID) {
                  prevSID = aPM.patientSID;
                  result++;
               }
               String selStr = createSelectStr(aPM);
               //logger.debug(" countRows " + countRows + " selStr " + selStr );
               // string, includes calculating daily_dose
               if (selStr != null) {                 
                 countRows += 1;
                 if (countRows%100 == 0) {
                   LogUtility.warn(" IdentifyPatients. countRows: " + countRows + " patientSID: " + aPM.patientSID);
                   System.out.println(" IdentifyPatients. countRows: " + countRows + " patientSID: " + aPM.patientSID);
                 }
               } else {
                 return;
               }
               if (countRows < BUFFER_SIZE) {
                   insStat.append(selStr).append(unionAll);
               } else if (countRows == BUFFER_SIZE) {
                   insStat.append(selStr);   // for last select,no need to have "union all"
                   LogUtility.debug(insStat);
                   _dest.update(insStat.toString());
                   int lenStat = insStat.length();
                   insStat.delete(0, lenStat);
                   insStat.append(insertRoot);
                   countRows = 0;
               }               
           }
           
           public void post(){
              // Here to save of last statements saved in insStat, if there are any
              if (insStat != null && insStat.length() > insertRoot.length()) {
                // we must take out the last unionAll which has 11 characters
                int lenDesired = insStat.length() - unionAll.length();
                String lastInsert = insStat.substring(0, lenDesired);
                //anotherSrcConn._update(lastInsert.toString());
                _dest.update(lastInsert.toString());
              }
              System.out.println("Identify Patients. Number: " + result);
              LogUtility.warn("IdentifyPatients. Number : " + result);
           }
           //public int getResultCount() { return result; }
        });
        return 0;
    }    
    
    private String createSelectStr(PtPerformanceMeasure aPM) {
        if (aPM == null) {
            return null;
        }
        // Here to create the select string
        StringBuffer strBuf = new StringBuffer("select ");
        strBuf.append(aPM.sta3n).append(COMMA);
        strBuf.append(aPM.patientSID).append(COMMA);
        strBuf.append(DBC.fixString(aPM.patientSSN)).append(COMMA);
        strBuf.append(DBC.fixString(aPM.patientIEN)).append(COMMA);
        strBuf.append(DBC.fixString(aPM.patientName)).append(COMMA);

        strBuf.append(DBC.fixTimestamp(aPM.dateOfBirth)).append(COMMA);
        strBuf.append(DBC.fixString(aPM.race)).append(COMMA);
        strBuf.append(DBC.fixString(aPM.sex)).append(COMMA);
        //strBuf.append(aPM.BP_PerformanceMeasureKey).append(COMMA);
        //strBuf.append(aPM.DMStatus).append(COMMA);
        //strBuf.append(aPM.HTNStatus).append(COMMA);
        //strBuf.append(aPM.A1cLessThan9_PerformanceMeasureKey).append(COMMA);
        //strBuf.append(aPM.DM_Nephropathy_Key).append(COMMA);
        //strBuf.append(aPM.LDLLessThan100_PerformanceMeasureKey).append(COMMA);
        //strBuf.append(aPM.IHDStatus).append(COMMA);

        strBuf.append(DBC.fixString(aPM.guidelinesToDoMap)).append(COMMA);
        // remove last COMMA
        return strBuf.substring(0, strBuf.length() - 1);
    }

    public void insertConditions() {
        if (!_properties.runThisFunction()) {
            return;
        }
        LogUtility.info("* Start insert AthenaConditions....");
        //String srcDB = _properties.getProperty("SOURCE_DB");         
        String destDB = _properties.getProperty("DEST_DB"); 
        
        _dest.update("DELETE FROM dbo.AthenaConditions");
        String q =
                " insert into " + destDB + ".dbo.AthenaConditions (sta3n, patientSID, SSN, icd9_1, last_date, Beginning, ICD9Description) "
                + " select dia.sta3n,           "
                + "        dia.patientSID,      "
                + "        pt.SSN,              "
                + "        icdref.ICDCode,      "
                + "        dia.VisitDateTime,   "
                + "        dia.VDiagnosisDateTime, "
                + "        icdref.ICDDescription   "
                + " from Outpat.VDiagnosis dia,    "
                + "      " + destDB + ".dbo.AthenaDemographics pt, "
                + "      dim.icd icdref                        "
                + " where dia.sta3n = pt.sta3n           "
                + "   and dia.patientSID = pt.patientSID "
                + "   and icdref.sta3n = dia.sta3n       "
                + "   and icdref.icdsid = dia.icdsid     ";
        _src.update(q);
        System.out.println(" inserted into AthenaConditions from VDiagnosis ");        
        LogUtility.warn(" inserted into AthenaConditions from VDiagnosis ");
        q =
                " insert into " + destDB + ".dbo.AthenaConditions (sta3n, patientSID, SSN, icd9_1, last_date, Beginning, ICD9Description) "
                + " select dia.sta3n,           "
                + "        dia.patientSID,      "
                + "        pt.SSN,              "
                + "        icdref.ICDCode,      "
                + "        dia.EnteredDate,     "
                + "        dia.OnsetDate,       "
                + "        icdref.ICDDescription   "
                + " from Outpat.ProblemList dia,   "
                + "      " + destDB + ".dbo.AthenaDemographics pt, "
                + "      dim.icd icdref                        "
                + " where dia.sta3n = pt.sta3n           "
                + "   and dia.patientSID = pt.patientSID "
                + "   and icdref.sta3n = dia.sta3n       "
                + "   and icdref.icdsid = dia.icdsid     ";
        _src.update(q);
        System.out.println(" inserted into AthenaConditions from ProblemList ");        
        LogUtility.warn(" inserted into AthenaConditions from ProblemList ");
    }

    
    public void insertAllergies() {
        if (!_properties.runThisFunction()) {
            return;
        }
        LogUtility.info("* Start insert AthenaAllergies....");
        _dest.update("DELETE FROM dbo.AthenaAllergies");
        String destDB = _properties.getProperty("DEST_DB");
        String q =
                " insert into " + destDB + ".dbo.AthenaAllergies (sta3n, patientSID,substance,reaction,originationDateTime) "
                + " select al.sta3n,           "
                + "        al.patientsid,      "
                + "        mpd.KBName as substance,   "
                + "        mpr.KBName as reaction,    "
                + "        al.OriginationDateTime   "
                + " from Allergy.Allergy al    "
                + "   join " + destDB + ".dbo.AthenaDemographics pt on "
                + "      al.sta3n = pt.sta3n and        "
                + "      al.patientSID = pt.patientSID  " 
                + "   join Allergy.AllergyDrugIngredient adi on "
                + "      al.sta3n = adi.sta3n and        "                                
                + "      al.AllergySID = adi.AllergySID        "
                + "   join Dim.DrugIngredient dimdi on "
                + "      adi.sta3n = dimdi.sta3n and        "
                + "      adi.DrugIngredientSID = dimdi.DrugIngredientSID    "
                + "   join " + destDB + ".dbo.AutoPMMap_ADRReactant mpd on "
                + "      mpd.KBName = dimdi.drugingredient     "
                + "   join Allergy.AllergicReaction ar on "
                + "      al.sta3n = ar.sta3n and    "
                + "      al.AllergySID = ar.AllergySID    "  
                + "   join Dim.Reaction dimr on  "
                + "      ar.sta3n = dimr.sta3n and  "
                + "      ar.ReactionSID = dimr.ReactionSID    "                
                + "   join " + destDB + ".dbo.AutoPMMap_ADRReaction mpr on  "
                + "       mpr.Reaction = dimr.Reaction  ";     
        _src.update(q);
        LogUtility.warn(" inserted into AthenaAllergies ");
    }    
    
    public void insertStudies() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        _dest.update("DELETE FROM dbo.AthenaStudies");
        String destDB = _properties.getProperty("DEST_DB");        
        String query =
                "insert into " + destDB + ".dbo.AthenaStudies \n" 
                + "(sta3n,patientSID,lab,value,unit,Timestamp) \n"
                + "select distinct pt.sta3n, \n"
                + "       pt.patientSID, \n"
                + "       mpl.KBName, \n"
                + "       lab.LabChemResultValue, \n"
                + "       lref.units, \n"
                + "       lab.LabChemSpecimenDateTime \n"
                + "from Chem.LabChem lab \n"
                + "  join " + destDB + ".dbo.AthenaDemographics pt on \n"
                + "    pt.sta3n = lab.sta3n and \n"
                + "    pt.patientSID = lab.patientSID \n"
                + "  join DIM.LOINC lref on \n"
                + "    lref.sta3n = lab.sta3n and \n"
                + "    lref.loincSID = lab.loincSID \n" 
                + "  join " + destDB + ".dbo.AutoPMMap_Labs mpl on \n"
                + "    mpl.loinc = lref.loinc \n"                
                + "order by pt.sta3n,pt.patientSID,mpl.KBName \n";
        _src.update(query);
        t.print();
    }
    
    public void insertVitals() {
    
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        _dest.update("DELETE FROM dbo.AthenaVitals");
        String destDB = _properties.getProperty("DEST_DB");         
        String query =
                "SELECT \n"
                + "     pt.sta3n \n"
                + "     ,pt.patientSID \n"
                + "     ,pt.SSN \n"
                + "     ,vtl.VitalType \n"
                + "     ,vmp.KBName \n"                
                + "     ,vtl.Systolic \n"
                + "     ,vtl.Diastolic \n"
                + "     ,vtl.ResultNumeric \n"
                + "     ,vtl.VitalSignTakenDateTime \n"
                + "FROM "
                + destDB + ".dbo.AthenaDemographics pt, \n"
                + destDB + ".dbo.AutoPMMap_Vitals vmp, \n"                            
                + " vital.vitalSign vtl \n"
                + "WHERE vtl.VitalType = vmp.vitalsType and \n"
                + "     vtl.sta3n = pt.sta3n and \n"
                + "     vtl.patientSID = pt.patientSID";
        //LogUtility.warn(" insertVitals query: " + query);
        
        _src.query(query, new SQLObject() {           
           int count= 0;
           java.lang.StringBuilder insert = new java.lang.StringBuilder();           
          
           public void row(ResultSet rs) throws SQLException {
              if (count < BUFFER_SIZE) {
                 insert.append(_getVitalInsert(rs));
                 count++;
                 if (count%1000 == 0) {
                   System.out.println(" insertVitals. Count:" + count)   ;
                 }
              } 
              else {
                 // do insert and clear buffer
                 _dest.update(insert.toString());
                 count = 0;  
                 insert.delete(0, insert.length());
                 insert.append(_getVitalInsert(rs));                                           
              }              
           } 
           
           public void post(){ 
             if (insert.length() > 0) {
               _dest.update(insert.toString());                   
             }              
           }                                            
        } );
        t.print();        
    }
        

    private String _getVitalInsert(ResultSet rs) {
        String result = "";
        String sta3n, patientSID, SSN, VitalType, KBName, Systolic, Diastolic, ResultNumeric;
        java.sql.Timestamp VitalSignTakenDateTime;
        try {
            sta3n = rs.getString("sta3n");
            patientSID = rs.getString("patientSID");
            SSN = rs.getString("SSN");
            VitalType = rs.getString("VitalType");
            KBName = rs.getString("KBName");            
            Systolic = rs.getString("Systolic");
            Diastolic = rs.getString("Diastolic");
            ResultNumeric = rs.getString("ResultNumeric");
            VitalSignTakenDateTime = rs.getTimestamp("VitalSignTakenDateTime");
        } catch (java.sql.SQLException e) {
            System.err.println("_getVitalInsert - " + e);
            return "";
        }
        if (VitalType.equalsIgnoreCase("BLOOD PRESSURE")) {
            result +=
                    "INSERT INTO dbo.AthenaVitals (sta3n, patientSID, SSN, measurement, units, value, Timestamp) \n"
                    + "VALUES ("
                    + DBC.fixString(sta3n) + ","
                    + DBC.fixString(patientSID) + ","
                    + DBC.fixString(SSN) + ","
                    + "'Systolic_BP',"
                    + "'mm',"
                    + DBC.fixString(Systolic) + ","
                    + DBC.fixTimestamp(VitalSignTakenDateTime) + ") \n";
            result +=
                    "INSERT INTO dbo.AthenaVitals (sta3n, patientSID, SSN, measurement, units, value, Timestamp) \n"
                    + "VALUES ("
                    + DBC.fixString(sta3n) + ","
                    + DBC.fixString(patientSID) + ","
                    + DBC.fixString(SSN) + ","
                    + "'Diastolic_BP',"
                    + "'mm',"
                    + DBC.fixString(Diastolic) + ","
                    + DBC.fixTimestamp(VitalSignTakenDateTime) + ") \n";
        } else {
            result +=
                    "INSERT INTO dbo.AthenaVitals (sta3n, patientSID, SSN, measurement, units, value, Timestamp) \n"
                    + "VALUES ("
                    + DBC.fixString(sta3n) + ","
                    + DBC.fixString(patientSID) + ","
                    + DBC.fixString(SSN) + ","
                    + DBC.fixString(KBName) + ","
                    + "'',"
                    + DBC.fixString(ResultNumeric) + ","
                    + DBC.fixTimestamp(VitalSignTakenDateTime) + ") \n";
        }
        return result;
    }

    public void insertPrescriptions() {
        if (!_properties.runThisFunction()) {
            return;
        }
        insertPrescriptionHistory();
        insertPrescriptionsActive();
    }

    private void insertPrescriptionHistory() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        String destDB = _properties.getProperty("DEST_DB");         
        _dest.update("DELETE FROM " + destDB + ".dbo.AthenaPrescriptionHistory");
        String query =
                "INSERT INTO " + destDB + ".dbo.AthenaPrescriptionHistory \n"
                + "SELECT \n"
                + "	pt.sta3n \n"
                + "	,pt.patientSID \n"
                + "	,pt.SSN \n"
                + "	,rxp.RxNumber \n"
                + "	,rxp.RxStatus \n"
                + "	,rxp.IssueDate \n"
                + "	,rxp.providerSID \n"
                + "	,rxp.NationalDrugSID \n"
                + "	,rxp.NationalDrugIEN \n"
                + "	,rxp.DrugNameWithoutDose \n"
                + "	,dmp.name \n"
                + "	,dmp.displayName \n"
                + "	,fil.qty \n"
                + "	,fil.ExpirationDate \n"
                + "	,fil.FillDateTime \n"
                + "	,fil.ReleaseDateTime \n"
                + "	,fil.DaysSupply \n"
                + "	,(convert(float,(convert(numeric(10,2),fil.qty)*dmp.strengthNumeric/convert(numeric(10,2),fil.DaysSupply)))) AS dailyDose \n"
                + "	,fil.FillType \n"
                + "	,dmp.DosageForm \n"
                + "	,dmp.units \n"
                + "	,dmp.strength \n"
                + "	,dmp.strengthNumeric \n" 
                + "FROM \n"
                + "	RxOut.RxOutpatFill fil \n"
                + "	,RxOut.RxOutpat     rxp \n"
                + "	," + destDB + ".dbo.AthenaDemographics pt \n"
                + "	," + destDB + ".dbo.AutoPMMap_Drugs dmp \n"
                + "WHERE \n"
                + "	fil.sta3n = pt.sta3n \n"
                + "AND fil.patientSID = pt.patientSID \n"
                + "AND rxp.sta3n = fil.sta3n \n"
                + "AND rxp.patientSID = fil.patientSID \n"
                + "AND rxp.RxOutpatSID = fil.RxOutpatSID \n"
                + "AND rxp.NationalDrugIEN = dmp.NationalDrugIEN \n"
                + "ORDER BY pt.patientSID, fil.issueDate, fil.ReleaseDateTime";
        _src.update(query);
        LogUtility.info(" insertPrescriptionHistory completed ");        
        t.print();
    }

    private void insertPrescriptionsActive() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        String destDB = _properties.getProperty("DEST_DB");         
        _dest.update("DELETE FROM " + destDB + ".dbo.AthenaPrescriptionActive");
        String query =
                "INSERT INTO " + destDB + ".dbo.AthenaPrescriptionActive \n"
                + "SELECT distinct \n"
                + "	p1.sta3n \n"
                + "	,p1.patientSID \n"
                + "	,p1.SSN \n"
                + "	,p1.RxNumber \n"
                + "	,p1.RxStatus \n"
                + "	,p1.IssueDate \n"
                + "	,p1.providerSID \n"
                + "	,p1.NationalDrugSID \n"
                + "     ,p1.NationalDrugIEN \n"
                + "     ,p1.DrugNameWithoutDose \n"
                + "     ,p1.ingredient \n"
                + "	,p1.displayName \n"
                + "	,p1.quantity \n"
                + "	,p1.ExpireDate \n"
                + "	,p1.FillDate \n"
                + "	,p1.ReleaseDate \n"
                + "	,p1.DaysSupply \n"
                + "	,p1.dailyDose \n"
                + "	,p1.FillType \n"
                + "	,p1.DosageForm \n"
                + "	,p1.units \n"
                + "	,p1.strength \n"
                + "	,p1.strengthNumeric \n"
                + "FROM " + destDB + ".dbo.AthenaPrescriptionHistory p1 \n"
                + "  join " + destDB + ".dbo.generatedSessionDate sd on \n"
                + "    sd.sta3n = p1.sta3n and  \n"
                + "    sd.patientSID = p1.patientSID \n"                              
                + "WHERE \n"
                + "  ( \n"
                + "    ( p1.RxStatus = 'ACTIVE' OR p1.RxStatus = 'SUSPENDED') \n"
                + "    AND ( (p1.ExpireDate > sd.effSessionDate ) OR ((p1.ReleaseDate + p1.DaysSupply) > sd.effSessionDate ) ) \n"
                + "    AND NOT EXISTS ( SELECT * FROM " + destDB + ".dbo.AthenaPrescriptionHistory p2 \n"
                + "                     WHERE p2.sta3n = p1.sta3n \n"
                + "                       and p2.patientSID = p1.patientSID \n"
                + "                       and p2.RxNumber = p1.RxNumber \n"
                + "                       and p2.RxStatus = p1.RxStatus \n"
                + "                       and p2.NationalDrugIEN = p1.NationalDrugIEN \n"
                + "                       and p2.ReleaseDate > p1.ReleaseDate ) \n"
                + "  ) \n"
                + "  OR \n"
                + "  ( \n"
                + "    p1.RxStatus = 'EXPIRED' \n"
                + "    AND (p1.ReleaseDate + p1.DaysSupply) > sd.effSessionDate \n"
                + "    AND NOT EXISTS ( SELECT * FROM " + destDB + ".dbo.AthenaPrescriptionHistory p2 \n"
                + "                     WHERE p2.sta3n = p1.sta3n \n"
                + "                       and p2.patientSID = p1.patientSID \n"
                + "                       and p2.RxNumber = p1.RxNumber \n"
                + "                       and p2.RxStatus = p1.RxStatus \n"
                + "                       and p2.NationalDrugIEN = p1.NationalDrugIEN \n"                
                + "                       and p2.ReleaseDate > p1.ReleaseDate) \n"
                + "  )";
        _dest.update(query);
        LogUtility.info(" insertPrescriptionsActive completed ");        
        t.print();
    }

    public void insertHealthFactors() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        String srcDB = _properties.getProperty("SOURCE_DB"); 
        String destDB = _properties.getProperty("DEST_DB");         
        _dest.update("DELETE FROM " + destDB + ".dbo.AthenaHealthFactors");
        String query =
                "DELETE FROM " + destDB + ".dbo.AthenaHealthFactors \n"
                + "INSERT INTO " + destDB + ".dbo.AthenaHealthFactors \n"
                + "SELECT DISTINCT \n"
                + "	hf.Sta3n \n"
                + "	,hf.PatientSID \n"
                + "	,pt.SSN \n"
                + "	,hf.HealthFactorType \n"
                + "	,hf.LevelSeverity \n"
                + "	,hf.HealthFactorDateTime \n"
                + "FROM \n"
                + " " + srcDB + ".HF.HealthFactor hf \n"
                + " join " + destDB + ".dbo.AthenaDemographics pt on \n"
                + "	hf.sta3n = pt.sta3n and \n"
                + "	hf.PatientSID = pt.PatientSID \n"
                + " order by hf.sta3n, hf.patientSID ";
        _dest.update(query);
        LogUtility.info(" insertHealthFactors completed ");
        t.print();
    }

    public void insertExams() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        String srcDB = _properties.getProperty("SOURCE_DB");         
        String destDB = _properties.getProperty("DEST_DB"); 
        
        _dest.update("DELETE FROM " + destDB + ".dbo.AthenaExams");
        String query =
                "INSERT INTO " + destDB + ".dbo.AthenaExams \n"
                + "SELECT \n"
                + "	e.Sta3n \n"
                + "	,e.PatientSID \n"
                + "	,e.VisitSID \n"
                + "	,e.VisitDateTime \n"
                + "	,e.VExamDateTime \n"
                + "	,e.ExamSID \n"
                + "	,x.Exam \n"
                + "	,e.AbnormalNormal \n"
                + "FROM \n"
                + "	" + srcDB + ".Outpat.VExam e \n"
                + "	," + destDB + ".Dim.Exam x \n"
                + "WHERE e.ExamSID = x.ExamSID";
        _dest.update(query);
        LogUtility.info(" insertExams completed ");        
        t.print();
    }

    public void insertProcedures() {
        if (!_properties.runThisFunction()) {
            return;
        }
        Timer t = Timer.start();
        String srcDB = _properties.getProperty("SOURCE_DB");         
        String destDB = _properties.getProperty("DEST_DB");         
        _dest.update("DELETE FROM " + destDB + ".dbo.AthenaProcedures");
        String query =
                "INSERT INTO " + destDB + ".dbo.AthenaProcedures \n"
                + "SELECT distinct \n"
                + "     p.Sta3n \n"
                + "     ,p.PatientSID \n"
                + "     ,p.VProcedureDateTime \n"
                + "     ,ref.CPTCode \n"
                + "     ,ref.CPTDescription \n"
                + "     ,mp.KBName \n"
                + "FROM \n"
                + "     " + srcDB + ".Outpat.VProcedure p \n"
                + "  join " + srcDB + ".Dim.CPT ref on \n"
                + "    ref.sta3n = p.sta3n and \n"               
                + "    ref.CPTSID = p.CPTSID \n"
                + "  join " + destDB + ".dbo.AutoPMMap_CPTCodes mp on \n"
                + "    mp.CPTCode = ref.CPTCode \n";
                
        _dest.update(query);
        LogUtility.info(" insertProcedures completed ");        
        t.print();
    }

    public void updateGoalForHTN(String BPDataString, int advisoryID) {

        String destDB = _properties.getProperty("DEST_DB");
        StringBuffer updateGoalPtData = new StringBuffer(" Update ");
        updateGoalPtData.append(destDB).append(".dbo.AthenaAdvGoal ");
        updateGoalPtData.append("set patientData=").append("\'").append(BPDataString).append("\'  ");
        updateGoalPtData.append("where fk_advisoryID=").append(advisoryID);
        LogUtility.warn(" updateGoalForHTN: " + updateGoalPtData.toString());
        _dest.update(updateGoalPtData.toString());
    }

    public java.util.TreeMap<String, PtPerformanceMeasure> getPatients() {
        
        final java.util.TreeMap<String, PtPerformanceMeasure> ptMap = new java.util.TreeMap();
        
        String srcDB = _properties.getProperty("SOURCE_DB");         
        String destDB = _properties.getProperty("DEST_DB");         
        String query =
                "SELECT dem.sta3n, dem.patientSID, dem.SSN, dem.patientDFN, \n"
                + " dem.name, dem.DOB, dem.race, dem.sex, dem.guidelinesToDoMap, sd.effSessionDate  \n"
                + "FROM " + destDB + ".dbo.AthenaDemographics dem \n"
                + " left outer join " + destDB + ".dbo.generatedSessionDate sd on \n"
                + "    sd.sta3n = dem.sta3n and \n" 
                + "    sd.patientSID = dem.patientSID \n"                
                + "order by dem.sta3n, dem.patientSID ";
        LogUtility.debug( "getPatientsQuery: " + query );
        
        _dest.query(query, new SQLObject() {                                              
            public void row(ResultSet rs) throws SQLException {
               String ptID = rs.getString("sta3n") + "_" + rs.getString("patientSID");
               PtPerformanceMeasure ptData = new PtPerformanceMeasure(
                            rs.getInt("sta3n"),
                            rs.getInt("patientSID"),
                            rs.getString("SSN"),
                            rs.getString("patientDFN"),
                            rs.getString("name"),
                            rs.getTimestamp("DOB"),
                            rs.getString("race"),
                            rs.getString("sex"),
                            rs.getString("guidelinesToDoMap"),
                            rs.getTimestamp("effSessionDate"));
              ptMap.put(ptID, ptData);               
            }                
          } );    
      
        return ptMap;        
    }

    /*
     * Delete the Athena Advisories for a selected set of patients, leaving
     * all other patients' advisory results alone
     * Each patient is identified by a string consisting of the concatenation
     * of sta3n, an underscore, and the patientSID, e.g. 640_100005555
     */
    public void deleteSelectedAdvisories(HashSet<String> setOfPts, HashSet<String> guidelineIDs,
                                         HashMap<String,String> mapGuidelineIDsToStrings) { 
        if (setOfPts == null || setOfPts.size() <= 0) {
            return;
        }
                
        ArrayList<Integer> lstAdvisoryIDs = determineListOfAdvisoriesToDelete(setOfPts, guidelineIDs, mapGuidelineIDsToStrings);
        if (lstAdvisoryIDs == null || lstAdvisoryIDs.size() <=0 ) {
           return;           
        }
        String destDB = _properties.getProperty("DEST_DB");
        // delete from AthenaAdvReferences  
        StringBuffer del = new StringBuffer();
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvReference ");
        del.append("where fk_evalRelationHID in ");
        del.append("( select evalRelationHID ");
        del.append("  from ").append(destDB).append(".dbo.AthenaAdvDrugRecommendation dr ");
        del.append("    join ").append(destDB).append(".dbo.AthenaAdvDrugEvalRelation  eval ");
        del.append("      on eval.fk_drugRecHID = dr.drugRecHID ");       
        String advClause = createAdvisoriesClause("dr", "fk_advisoryID", lstAdvisoryIDs);
        del.append("  and ").append(advClause).append(" )");  
        LogUtility.debug(" del: " + del.toString());
        _dest.update(del.toString());
        
        // Delete from AthenaAdvDrugEvalRelation 
        del.delete(0, del.length()) ;
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvDrugEvalRelation ");
        del.append("where evalRelationHID in ");
        del.append("( select evalRelationHID ");
        del.append("  from ").append(destDB).append(".dbo.AthenaAdvDrugRecommendation dr ");
        del.append("    join ").append(destDB).append(".dbo.AthenaAdvDrugEvalRelation  eval ");
        del.append("      on eval.fk_drugRecHID = dr.drugRecHID ");       
        advClause = createAdvisoriesClause("dr", "fk_advisoryID", lstAdvisoryIDs);
        del.append("  and ").append(advClause).append(" )"); 
        LogUtility.debug(" AthenaAdvDrugEvalRelation del: " + del.toString());        
        _dest.update(del.toString());
        
        // Delete from AthenaAdvDrugs 
        del.delete(0, del.length()) ;
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvDrugs ");
        del.append("where fk_drugRecHID in ");
        del.append("( select drg.fk_drugRecHID ");
        del.append("  from ").append(destDB).append(".dbo.AthenaAdvDrugs drg ");
        del.append("    join ").append(destDB).append(".dbo.AthenaAdvDrugRecommendation dr ");
        del.append("      on drg.fk_drugRecHID = dr.drugRecHID ");       
        advClause = createAdvisoriesClause("dr", "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause).append(" )");
        LogUtility.debug(" del: " + del.toString());        
        _dest.update(del.toString());
        
        // Delete from AthenaAdvDrugRecommendation 
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvDrugRecommendation ");     
        advClause = createAdvisoriesClause(null, "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);    
        LogUtility.warn(" AthenaAdvDrugRecommendation del: " + del.toString());        
        _dest.update(del.toString()); 
        
        // Delete from AthenaAdvAction (all messages, referrals and orders)
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvAction  ");     
        advClause = createAdvisoriesClause(null, "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);        
        LogUtility.debug(" del: " + del.toString());        
        _dest.update(del.toString());         
        
        // Delete from AthenaAdvGoal
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvGoal ");     
        advClause = createAdvisoriesClause(null, "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);        
        LogUtility.debug(" del: " + del.toString());        
        _dest.update(del.toString());            

        // Delete from AthenaAdvConclusion
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvConclusion ");     
        advClause = createAdvisoriesClause(null, "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);        
        LogUtility.debug(" del: " + del.toString());        
        _dest.update(del.toString());          
        
        // Delete from AthenaAdvDrugRecDisplay
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvDrugRecDisplay ");     
        advClause = createAdvisoriesClause(null, "fk_advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);        
        LogUtility.debug(" AthenaAdvDrugRecDisplay del: " + del.toString());        
        _dest.update(del.toString());  
        
        // Delete from AthenaAdvKey
        del.delete(0, del.length());
        del.append("delete from ").append(destDB).append(".dbo.AthenaAdvKey ");     
        advClause = createAdvisoriesClause(null, "advisoryID", lstAdvisoryIDs);
        del.append("  where ").append(advClause);        
        LogUtility.debug(" AthenaAdvKey del: " + del.toString());        
        _dest.update(del.toString());                           
    }
    
    //
    // This method will construct a series of where clauses for 
    // the list of advisoryID's of the form:
    //       (table_alias.advisoryID_columnName = lstAdvisoryIDs.get(0)  
    //   or  table_alias.advisoryID_columnName = lstAdvisoryIDs.get(1)  
    //   .... )
    // e.g. 
    //       (dr.fk_advisoryID = 190000001
    //    or  dr.fk_advisoryID = 180000001 )
    // 
    private String createAdvisoriesClause(String tableAlias, String advIDColumnName, java.util.List lstAdvisoryIDs) {
       if (advIDColumnName == null || lstAdvisoryIDs == null || lstAdvisoryIDs.size()<=0) {
           return null;
       }
       // insert beginning parenthesis
       String whereClauseRes = "( ";
       String condition;
       if (tableAlias == null || tableAlias.length() <=0 ) {
          condition =  advIDColumnName + " = ";
       }
       else {
          condition = tableAlias + "." + advIDColumnName + " = ";
       }
       
       for (int k=0; k<lstAdvisoryIDs.size(); k++) {
         String anAdvID = lstAdvisoryIDs.get(k).toString();
         if (k == 0 ) {
            whereClauseRes += condition + anAdvID;
         } else {
            whereClauseRes += " or " + condition + anAdvID;
         }
       }
       
       // add enclosing parenthesis
       whereClauseRes += " )";
       
       return whereClauseRes;
    }
    
    // Here to obtain the list of advisory ID's, integers, for the patients selected 
    // to be processed
    private ArrayList<Integer> determineListOfAdvisoriesToDelete(HashSet<String> setOfPts, HashSet<String> guidelineIDs,
                                                                 HashMap<String,String> mapGuidelineIDsToStrings) {
       
        final ArrayList<Integer> resultLst = new ArrayList<Integer>();

        if (setOfPts == null || setOfPts.size() <= 0) {
            return null;
        }

        /* We shall construct a select statement of the form
         *   select advisoryID from AthenaAdvKey
         *   where (sta3n = @sta3n1 and patientSID = @patientSID1 and guidelineID = @guidelineID ) 
         *      or (sta3n = @sta3n2 and patientSID = @patientSID2 and guidelineID = @guidelineID )
         * Each String in clauses is of the form:
         *    (sta3n = @sta3n1 and patientSID = @patientSID1 and guidelineID = @guidelineID )
         * when there is only 1 guidelineID.                   
         */        
        String destDB = _properties.getProperty("DEST_DB");
        ArrayList<String> clauses = new ArrayList<String>();
        Iterator iter = setOfPts.iterator();
        
        while (iter.hasNext()) {
            String aPtID = (String) iter.next();    // aPtID = sta3n_patientSID
            java.util.List<String> components = DataSourceAutoPM.getDashDelimitedList(aPtID);
            if (components != null && components.size() == 2) {
                String sta3n = components.get(0);
                String ptSID = components.get(1);
                //LogUtility.warn(" ptSID: " + ptSID);
                
                Iterator itGuidelines = guidelineIDs.iterator();                
                while (itGuidelines.hasNext()) {
                   String aGuidelineID = (String) itGuidelines.next();
                   String guidelineIDString = (String) mapGuidelineIDsToStrings.get(aGuidelineID);
                   //if (guidelineIDString == null || guidelineIDString.length() <= 0 ) {
                   //    continue;
                   //}
                   //LogUtility.warn(" ptSID:" + ptSID + " guidelineID: " + guidelineIDString);
                   StringBuffer aClause = new StringBuffer("( sta3n=").append(sta3n);
                   aClause.append(" and patientSID=").append(ptSID);
                   aClause.append(" and guidelineID=\'").append(guidelineIDString).append("\'");
                   aClause.append("  )");                   
                   clauses.add(aClause.toString());
                }
                
            } else {
                LogUtility.error(" Cannot obtain sta3n,ptSID components for ptID: " + aPtID);
            }
        }

        //LogUtility.warn(" numberClauses: " + clauses.size());
        StringBuffer advQuery = new StringBuffer("select advisoryID from ");
        advQuery.append(destDB).append(".dbo.AthenaAdvKey \n");
        advQuery.append("where ");
        for (int j = 0; j < clauses.size(); j++) {
            String aClause = clauses.get(j);
            if (j == 0) {
                advQuery.append(aClause);
            } else {
                advQuery.append(" or ").append(aClause);
            }
        }
        
        LogUtility.warn(" Query for advisoryID's to delete: " + advQuery.toString());
        
        _dest.query(advQuery.toString(), new SQLObject() {                    
           public void row(ResultSet rs ) throws SQLException {                  
               int advisoryID = rs.getInt("advisoryID");
               resultLst.add(new Integer(advisoryID));            
           }            
        });  
        
        if (resultLst != null || resultLst.size() > 0 ){
          StringBuffer lst = new StringBuffer();
          for (int k=0; k<resultLst.size(); k++) {
            lst.append( (resultLst.get(k)).toString()).append(" "); 
          }
          LogUtility.warn(" advisoryID's to delete: " + lst.toString());
        } 
        return resultLst;
        
    }
    
    public int getMaxAdvisorySequenceID() {
       
       String destDB = _properties.getProperty("DEST_DB");
       String query = "select MAX(advisoryID) as maxID from " + destDB + ".dbo.AthenaAdvKey ";        
       final ArrayList<Integer> lstAdvisoryID = new ArrayList<Integer>(1);  // for storing max advisory ID
       
       _dest.query(query, new SQLObject() {
          int result = 200000000; 
          public void row(ResultSet rs) throws SQLException {
             result = rs.getInt("maxID"); 
             lstAdvisoryID.add(new Integer(result));
          }
       });       
       Integer maxAdvisoryID = lstAdvisoryID.get(0);
       return maxAdvisoryID.intValue();
    }    
}
