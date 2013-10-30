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
package spiralcraft.sql.vendor.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import spiralcraft.sql.Dialect;
import spiralcraft.sql.SqlType;
import spiralcraft.sql.types.RealAsDoubleType;
import spiralcraft.sql.util.Session;

public class MysqlDialect
  extends Dialect
{

  { setExtendedTypes(new SqlType<?>[] { new RealAsDoubleType() });
  }
  
  /**
   * The default schema name to use for application tables if no specific
   *   schema name is specified.
   * 
   * @return
   */
  @Override
  public String getDefaultSchemaName()
  { return null;
  }

  @Override
  public boolean isVarcharSizeRequired()
  { return true;
  }
  
  @Override
  public int getDefaultVarcharSize()
  { return 767;
  }
  
  @Override
  public void initConnection(Connection connection)
    throws SQLException
  { 
    Session session=new Session();
    session.start(connection);
    session.executeUpdate
      ("SET SESSION sql_mode='ANSI_QUOTES'"
      );
    
  }  

}
