//
//Copyright (c) 1998,2007 Michael Toth
//Spiralcraft Inc., All Rights Reserved
//
//This package is part of the Spiralcraft project and is licensed under
//a multiple-license framework.
//
//You may not use this file except in compliance with the terms found in the
//SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
//at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
//Unless otherwise agreed to in writing, this software is distributed on an
//"AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql;

import java.util.List;

/**
 * Represents a fragment of a Sql Statement to facilitate programmatic construction of
 *   statements
 */
public abstract class SqlFragment
{
  
  /**
   * <P>Write the SqlFragment and its children to the specified buffer.
   *  
   * <P>When a parameter is  encountered, add it to the parameterCollector.
   * @param parameterCollector TODO
   */
  public abstract void write(StringBuilder buffer,String indent, List parameterCollector);

  /**
   * <P>Generate the SQL text represented by the fragment
   * 
   * <P>When parameters are encountered as the Statement is serially written to text,
   *   the tag value supplied for each parameter will be added to the parameterCollector
   *   list so that the caller can associate a value to the appropriate parameter index.
   * 
   * @param parameterCollector
   * @return the SQL text
   */
  public String generateSQL(List parameterCollector)
  {
    StringBuilder buffer=new StringBuilder();
    write(buffer,"",parameterCollector);
    return buffer.toString();
  }
  
}
