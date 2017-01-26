/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */


package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import java.util.Locale;
import java.util.Vector;
import java.util.Enumeration;

public class AttributeHolder {

    //This is an inner class.  This class hold the details about each
    //specific attribute which includes what the attribute is and
    //any error found.
    String name;
    String value;
    String token;
    Vector errors = new Vector();

    public String getName(){
      return name;
    }
    public void setName(String aString){
      name = aString;
    }
    String getValue(){
      return value;
    }
    public void setValue(String aString){
      value = aString;
    }
    String getToken(){
      return token;
    }
    public void setToken(String aString){
      token = aString;
    }
    public void addError(String aString) {
      //Keep track of error message for later display.
      if (!errors.contains(aString))
        errors.addElement(aString);
    }
   public void check( Vector validProps){
      checkName( validProps);
      //checkValue();
      displayErrors();
    }
    void displayErrors(){
      //If no error are found then nothing is displayed.
      Enumeration e = errors.elements();
      //In the first line, show the exact token that was parsed from
      //the URL.
      if (e.hasMoreElements())
        display(LocalizedResource.getMessage("TL_urlLabel1", "[", getToken(), "]"));
      //Show all errors.  More than one error can be found for an attribute.
      while (e.hasMoreElements()){
        String aString = (String)e.nextElement();
        displayIndented(aString);
      }
    }
    void checkName( Vector validProps){
      if( validProps == null)
          return; // valid properties are unknown
      String anAtt = getName();
      try {
        //Check the found name against valid names.
        if (!validProps.contains(anAtt)) {
          //Check for case spelling of the name.
          if (validProps.contains(anAtt.toLowerCase(java.util.Locale.ENGLISH))) {
            errors.addElement(LocalizedResource.getMessage("TL_incorCase"));
          }
          //Check if this is even a valid attribute name.
          else {
            errors.addElement(LocalizedResource.getMessage("TL_unknownAtt"));
          }
        }
        else {
          //This Is a valid attribute.
        }
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    void checkValue(){
      String anAtt = getName(); 
      String aValue = getValue();
      try {
        //Check all attribute that require a boolean.
        if (URLCheck.getBooleanAttributes().contains(anAtt)) {
          if (!checkBoolean(aValue)) {
            errors.addElement(LocalizedResource.getMessage("TL_trueFalse"));
          }
        }
      }
      catch (Exception ex) {
        ex.printStackTrace();
      }
    }
	  boolean checkBoolean(String aValue) {
		  if (aValue == null)
			  return false;
		  return aValue.toLowerCase(Locale.ENGLISH).equals("true") || 
			  aValue.toLowerCase(Locale.ENGLISH).equals("false");
	  }
    void display(String aString) {
		LocalizedResource.OutputWriter().println(aString);
    }
    void displayIndented(String aString) {
		LocalizedResource.OutputWriter().println("   " + aString);
    }
  }
