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

import spiralcraft.sql.SqlFragment;

import java.util.ArrayList;
import java.util.List;

public class InsertStatement
    extends SqlFragment
{


  private TableName tableName;
  private ArrayList<String> columnNameList;
  private QueryExpression queryExpression;
  
  public void addColumnName(String columnName)
  { 
    if (columnNameList==null)
    { columnNameList=new ArrayList<String>();
    }
    columnNameList.add(columnName);
  }
  
  public void setQueryExpression(QueryExpression queryExpression)
  { this.queryExpression=queryExpression;
  }

  public void setTableName(TableName tableName)
  { this.tableName=tableName;
  }

  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("INSERT INTO ");
    tableName.write(buffer, indent, parameterCollector);
    
    indent=indent+"  ";

    if (columnNameList!=null)
    {
      buffer.append("\r\n").append(indent);
      buffer.append("(");
      
      boolean first=true;
      for (String column: columnNameList)
      { 
        if (first)
        { first=false;
        }
        else
        { buffer.append(",");
        }
        buffer.append(column);
        buffer.append("\r\n").append(indent);
      }
      buffer.append(")");
    }
        
    if (queryExpression!=null)
    { queryExpression.write(buffer,indent, parameterCollector);
    }

  }
}
