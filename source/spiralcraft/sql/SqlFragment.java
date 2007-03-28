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

  private SqlFragment parent;
  private List parameterCollector;
  
  public void setParent(SqlFragment parent)
  { this.parent=parent;
  }
  
  public SqlFragment getParent()
  { return parent;
  }
  
  /**
   * Write the SqlFragment and its children to the specified buffer
   */
  public abstract void write(StringBuilder buffer,String indent);

  /**
   * When parameters are encountered as the Statement is serially written to text,
   *   the tag value supplied for each parameter will be added to the list so that
   *   the caller can associate a value to the appropriate parameter index.
   */
  public void collectParameters(List list)
  { parameterCollector=list;
  }
  
  /**
   * Called when a parameter is encountered as the Statement is serially written to
   *   text. The tag is a marker that can be used by the caller to associate this parameter
   *   with a value when the statement is executed.
   */
  @SuppressWarnings("unchecked") // tag is opaque
  protected void parameterAssigned(Object tag)
  {
    if (parameterCollector!=null)
    { parameterCollector.add(tag);
    }
  }
}
