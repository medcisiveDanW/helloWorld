package gov.va.athena.advisory;

import java.util.*;

/**
 * Generated by Protege (http://protege.stanford.edu). Source Class: Message
 *
 * @version generated on Thu Jul 05 01:40:52 GMT 2012
 */
public interface Message extends Action {

    // Slot message
    String getMessage();

    boolean hasMessage();

    void setMessage(String newMessage);

    // Slot message_type
    String getMessage_type();

    boolean hasMessage_type();

    void setMessage_type(String newMessage_type);

    // Slot rule_in_criteria
    String getRule_in_criteria();

    boolean hasRule_in_criteria();

    void setRule_in_criteria(String newRule_in_criteria);

    boolean equals(Message aMsg);
}