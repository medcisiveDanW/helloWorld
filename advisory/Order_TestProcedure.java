package gov.va.athena.advisory;

/**
 * Generated by Protege (http://protege.stanford.edu). Source Class:
 * Order_TestProcedure
 *
 * @version generated on Thu Jul 05 01:40:52 GMT 2012
 */
public interface Order_TestProcedure extends Action {

    // Slot code
    String getCode();

    boolean hasCode();

    void setCode(String newCode);

    // Slot coding_system
    String getCoding_system();

    boolean hasCoding_system();

    void setCoding_system(String newCoding_system);

    // Slot test_or_procedure
    String getTest_or_procedure();

    boolean hasTest_or_procedure();

    void setTest_or_procedure(String newTest_or_procedure);

    // Slot when
    String getWhen();

    boolean hasWhen();

    void setWhen(String newWhen);

    // Slot when_lower_bound
    int getWhen_lower_bound();

    boolean hasWhen_lower_bound();

    void setWhen_lower_bound(int newWhen_lower_bound);

    // Slot when_lower_bound_unit
    String getWhen_lower_bound_unit();

    boolean hasWhen_lower_bound_unit();

    void setWhen_lower_bound_unit(String newWhen);

    // Slot when_upper_bound
    int getWhen_upper_bound();

    boolean hasWhen_upper_bound();

    void setWhen_upper_bound(int newWhen_upper_bound);

    // Slot when_upper_bound_unit
    String getWhen_upper_bound_unit();

    boolean hasWhen_upper_bound_unit();

    void setWhen_upper_bound_unit(String newWhen);

    boolean equals(Order_TestProcedure anOrder);

}