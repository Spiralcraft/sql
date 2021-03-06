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


public class CreateTableStatement
    extends DDLStatement
{
  
  private String schemaName;
  private String tableName;
  private TableElementList elements;
  
  public CreateTableStatement
    (String schemaName
    ,String tableName
    ,TableElementList elements
    )
  {
    this.schemaName=schemaName;
    this.tableName=tableName;
    this.elements=elements;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    buffer.append("\r\n").append(indent);
    
    buffer.append("CREATE TABLE ");
    
    buffer.append(dialect.getQualifiedTableName(null,schemaName,tableName));
    
    indent=indent+"  ";
    
    elements.write(buffer,indent, parameterCollector);
    
  }
}
