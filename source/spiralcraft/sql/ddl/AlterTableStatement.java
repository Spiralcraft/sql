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


public class AlterTableStatement
    extends DDLStatement
{
  
  private String schemaName;
  private String tableName;
  private AlterTableAction action;
  
  public AlterTableStatement
    (String schemaName
    ,String tableName
    ,AlterTableAction action
    )
  {
    this.schemaName=schemaName;
    this.tableName=tableName;
    this.action=action;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("ALTER TABLE ");
    if (schemaName!=null)
    { buffer.append(schemaName).append(".");
    }
    buffer.append("\"").append(tableName).append("\"").append(" ");
    
    indent=indent+"  ";
    
    action.write(buffer,indent, parameterCollector);
    
  }
}
