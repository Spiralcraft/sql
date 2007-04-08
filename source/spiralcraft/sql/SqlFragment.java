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
  
  protected void add(SqlFragment child)
  { child.setParent(this);
  }
  
  void setParent(SqlFragment parent)
  { this.parent=parent;
  }
  
  /**
   * Write the SqlFragment and its children to the specified buffer
   */
  public abstract void write(StringBuilder buffer,String indent);

  public String generateSQL()
  {
    StringBuilder buffer=new StringBuilder();
    write(buffer,"");
    return buffer.toString();
  }
  
  /**
   * When parameters are encountered as the Statement is serially written to text,
   *   the tag value supplied for each parameter will be added to the list so that
   *   the caller can associate a value to the appropriate parameter index.
   */
  public void collectParameters(List list)
  { parameterCollector=list;
  }
  
  /**
   * Propogate a parameter up to the root statement of this SqlFragment tree.
   */
  @SuppressWarnings("unchecked") // Not using generics here
  void collectParameter(Object tag)
  {
    if (parameterCollector!=null)
    { parameterCollector.add(tag);
    }
    else if (parent!=null)
    { parent.collectParameter(tag);
    }
    else
    { throw new UnsupportedOperationException("SqlFragment: No way to collect parameter");
    }
  }
  
  /**
   * Called when a parameter is encountered as the Statement is serially written to
   *   text. The tag is a marker that can be used by the caller to associate this parameter
   *   with a value when the statement is executed.
   */
  @SuppressWarnings("unchecked") // tag is opaque
  protected void parameterAssigned(Object tag)
  { collectParameter(tag);
  }
}
