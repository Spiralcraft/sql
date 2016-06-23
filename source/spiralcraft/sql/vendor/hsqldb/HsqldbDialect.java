//
// Copyright (c) 2009 Michael Toth
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
package spiralcraft.sql.vendor.hsqldb;


import spiralcraft.sql.Dialect;
import spiralcraft.sql.SqlType;
import spiralcraft.sql.types.RealAsDoubleType;

public class HsqldbDialect
  extends Dialect
{

  { 
    setExtendedTypes(new SqlType<?>[] { new RealAsDoubleType() });
  }
  
  /**
   * The default schema name to use for application tables if no specific
   *   schema name is specified.
   * 
   * @return
   */
  @Override
  public String getDefaultSchemaName()
  { return "PUBLIC";
  }
  
  @Override
  public void writeAlterColumnTypeDDL(String columnName,StringBuilder buffer)
  {
    buffer
      .append("ALTER COLUMN ")
      .append("\"")
      .append(columnName)
      .append("\"")
      .append(" SET DATA TYPE ");

  }

}
