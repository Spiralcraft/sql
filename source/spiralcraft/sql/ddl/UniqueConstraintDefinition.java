//
// Copyright (c) 1998,2007 Michael Toth
// Spiralcraft Inc., All Rights Reserved
//
// This package is part of the Spiralcraft project and is licensed under
// a multiple-license framework.
//
// You may not use this file except in compliance with the terms found in the
// SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
// at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
// Unless otherwise agreed to in writing, this software is distributed on an
// "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql.ddl;

import java.util.List;


public abstract class UniqueConstraintDefinition
    extends TableConstraint
{
  
  private String[] columnNameList;
  private String uniqueSpecification;
  
  protected UniqueConstraintDefinition
    (String uniqueSpecification,String[] columnNameList)
  { 
    this.columnNameList=columnNameList;
    this.uniqueSpecification=uniqueSpecification;
  
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append(uniqueSpecification).append(" (");
    boolean first=true;
    for (String col: columnNameList)
    {
      if (first)
      { first=false;
      }
      else
      { buffer.append(",");
      }
      buffer.append("\"").append(col).append("\"");
    }
    buffer.append(") ");
  }
}
