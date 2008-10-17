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


public class SelectStatement
    extends SimpleTable
{

  private SelectList selectList;
  private FromClause fromClause;
  private WhereClause whereClause;
  
  public void setSelectList(SelectList selectList)
  { this.selectList=selectList;
  }
  
  public void setFromClause(FromClause fromClause)
  { this.fromClause=fromClause;
  }

  public void setWhereClause(WhereClause whereClause)
  { this.whereClause=whereClause;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("SELECT ");
    
    indent=indent+"  ";

    if (selectList!=null)
    { selectList.write(buffer,indent+"  ", parameterCollector);
    }
    
    if (fromClause!=null)
    { fromClause.write(buffer,indent, parameterCollector);
    }
    
    if (whereClause!=null)
    { whereClause.write(buffer,indent, parameterCollector);
    }
  }
}
