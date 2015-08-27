package gov.va.athena.advisory;

import java.util.*;

/**
 * Generated by Protege (http://protege.stanford.edu). Source Class:
 * Add_Delete_Drug_Recommendation
 *
 * @version generated on Thu Jul 05 01:40:52 GMT 2012
 */
public interface Add_Delete_Drug_Recommendation extends Drug_Recommendation {

    // Slot associated_substitution_drug
    Add_Delete_Drug_Recommendation getAssociated_substitution_drug();

    boolean hasAssociated_substitution_drug();

    void setAssociated_substitution_drug(Add_Delete_Drug_Recommendation newAssociated_substitution_drug);

    // Slot drugToAddString
    String getDrug_to_add_string();

    boolean hasDrug_to_add_string();

    void setDrug_to_add_string(String newDrugToAddString);

    // Slot drug_class
    String getDrug_class();

    boolean hasDrug_class();

    void setDrug_class(String newDrug_class);

    // Slot evaluated_drug_relation
    Collection<Evaluated_Drug_Relation> getEvaluated_drug_relation();

    boolean hasEvaluated_drug_relation();

    void addEvaluated_drug_relation(Evaluated_Drug_Relation newEvaluated_drug_relation);

    void removeEvaluated_drug_relation(Evaluated_Drug_Relation oldEvaluated_drug_relation);

    void setEvaluated_drug_relation(Collection<? extends Evaluated_Drug_Relation> newEvaluated_drug_relation);

    boolean equals(Add_Delete_Drug_Recommendation anAddDelRec);

}
