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


public class DeleteStatement
  extends SqlFragment
{


  private TableName tableName;
  private WhereClause whereClause;
  
  
  public void setWhereClause(WhereClause whereClause)
  { 
    this.whereClause=whereClause;
  }

  public void setTableName(TableName tableName)
  { 
    this.tableName=tableName;
  }

  
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("DELETE FROM ");
    tableName.write(buffer, indent, parameterCollector);
    buffer.append(" ");
        
    if (whereClause!=null)
    { 
      buffer.append("WHERE ");
      whereClause.write(buffer,indent, parameterCollector);
    }

  }
}
