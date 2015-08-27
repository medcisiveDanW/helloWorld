package gov.va.athena.advisory;

import java.util.*;

/**
 * Generated by Protege (http://protege.stanford.edu). Source Class:
 * Evaluated_Drug_Relation
 *
 * @version generated on Thu Jul 05 01:40:52 GMT 2012
 */
public interface Evaluated_Drug_Relation extends Advisory_Component {

    // Slot condition_or_drug
    String getCondition_or_drug();

    boolean hasCondition_or_drug();

    void setCondition_or_drug(String newCondition_or_drug);

    // Slot substance (only useful for adverse reactions, where it is the drug or ingredient
    // which causes a reaction, which is captured in condition_or_drug
    String getSubstance();

    boolean hasSubstance();

    void setSubstance(String aSubstance);

    // Slot reference
    Collection<String> getReference();

    boolean hasReference();

    void addReference(String newReference);

    void removeReference(String oldReference);

    void setReference(Collection<String> newReference);

    // Slot relation_type
    String getRelation_type();

    boolean hasRelation_type();

    void setRelation_type(String newRelation_type);

}
