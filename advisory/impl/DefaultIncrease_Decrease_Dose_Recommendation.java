package gov.va.athena.advisory.impl;

import gov.va.athena.advisory.*;
import java.util.Collection;
import java.util.*;

/**
 * Generated by Protege (http://protege.stanford.edu). Source Class:
 * Increase_Decrease_Dose_Recommendation
 *
 * @version generated on Wed Jun 06 18:58:08 GMT 2012
 */
public class DefaultIncrease_Decrease_Dose_Recommendation extends DefaultDrug_Recommendation implements Increase_Decrease_Dose_Recommendation {

    protected Collection<Evaluated_Drug_Relation> evaluated_drug_relation = new ArrayList();

    public DefaultIncrease_Decrease_Dose_Recommendation() {
        preference = "preferred";
    }

    public Collection<Evaluated_Drug_Relation> getEvaluated_drug_relation() {
        if (evaluated_drug_relation.isEmpty()) {
            return null;
        } else {
            return evaluated_drug_relation;
        }
    }

    public boolean hasEvaluated_drug_relation() {
        return !evaluated_drug_relation.isEmpty();
    }

    public void addEvaluated_drug_relation(Evaluated_Drug_Relation newEvaluated_drug_relation) {
        evaluated_drug_relation.add(newEvaluated_drug_relation);
    }

    public void removeEvaluated_drug_relation(Evaluated_Drug_Relation oldEvaluated_drug_relation) {
        evaluated_drug_relation.remove(oldEvaluated_drug_relation);
    }

    public void setEvaluated_drug_relation(Collection<? extends Evaluated_Drug_Relation> newEvaluated_drug_relation) {
        evaluated_drug_relation.addAll(newEvaluated_drug_relation);
    }

    public boolean equals(Increase_Decrease_Dose_Recommendation anIncDecDoseRec) {
        return super.equals((Drug_Recommendation) anIncDecDoseRec);
    }

}
