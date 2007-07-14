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

public class UpdateStatement
  extends SqlFragment
{


  private TableName tableName;
  private ArrayList<SetClause> setClauseList;
  private WhereClause whereClause;
  
  public void addSetClause(SetClause setClause)
  { 
    if (setClauseList==null)
    { setClauseList=new ArrayList<SetClause>();
    }
    setClauseList.add(setClause);
  }
  
  public void setWhereClause(WhereClause whereClause)
  { this.whereClause=whereClause;
  }

  public void setTableName(TableName tableName)
  { this.tableName=tableName;
  }

  
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("UPDATE ");
    tableName.write(buffer, indent, null);
    
    indent=indent+"  ";

    if (setClauseList!=null)
    {
      buffer.append("\r\n").append(indent);
      buffer.append("SET");
      buffer.append("\r\n").append(indent);
      
      boolean first=true;
      for (SetClause setClause: setClauseList)
      { 
        if (first)
        { first=false;
        }
        else
        { buffer.append(",");
        }
        setClause.write(buffer,indent, parameterCollector);
        buffer.append("\r\n").append(indent);
      }
    }
        
    if (whereClause!=null)
    { whereClause.write(buffer,indent, parameterCollector);
    }

  }
}
