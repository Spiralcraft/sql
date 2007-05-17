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

package spiralcraft.sql.dml;

import java.util.List;

import spiralcraft.sql.SqlFragment;

public class TableReference
  extends SqlFragment
{
  private String schemaName;
  private String tableName;
  private DerivedTable derivedTable;
  private JoinedTable joinedTable;
  private String correlation;
  
  public TableReference(String schemaName,String tableName)
  { 
    this.tableName=tableName;
    this.schemaName=schemaName;
  }
  
  public TableReference(String schemaName,String tableName,String correlation)
  { 
    this.tableName=tableName;
    this.schemaName=schemaName;
    this.correlation=correlation;
  }

  public void write(StringBuilder buffer,String indent, List parameterCollector)
  {
    if (tableName!=null)
    { 
      if (schemaName!=null)
      { buffer.append("\"").append(schemaName).append("\".");
      }
      buffer.append("\"").append(tableName).append("\" ");
      if (correlation!=null)
      { buffer.append("AS ").append(correlation);
      }
    }
    else if (derivedTable!=null)
    { 
      indent=indent+"  ";
      derivedTable.write(buffer,indent, parameterCollector);
      if (correlation!=null)
      { 
        buffer.append("\r\n").append(indent);
        buffer.append("AS ").append(correlation).append(" ");
      }
    }
    else if (joinedTable!=null)
    { joinedTable.write(buffer,indent, parameterCollector);
    }
  }
}
