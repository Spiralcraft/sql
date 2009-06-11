//
//Copyright (c) 1998,2007 Michael Toth
//Spiralcraft Inc., All Rights Reserved
//
//This package is part of the Spiralcraft project and is licensed under
//a multiple-license framework.
//
//You may not use this file except in compliance with the terms found in the
//SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
//at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
//Unless otherwise agreed to in writing, this software is distributed on an
//"AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql;

import java.util.HashMap;

/**
 * Resolves language features that may differ between database vendors. Methods in this
 *   class may be overridden by database specific methods.
 */
public class Dialect
{
  
  private final HashMap<Integer,SqlType<?>> typeMap
    =new HashMap<Integer,SqlType<?>>();
    
  /**
   * Specify a set of extended types that will override the basic JDBC type mappings
   *   for specific databases. 
   */
  public void setExtendedTypes(SqlType<?>[] extendedTypes)
  { 
    for (SqlType<?> type: extendedTypes)
    { typeMap.put(type.getTypeId(), type);
    }
  }
  
  /**
   * @return The maximum number of characters that can be stored in a Varchar.
   */
  public int getMaximumVarcharSize()
  { return 2048;
  }
  
  /**
   *@return an appropriate SqlType for this JDBC type id. Will consult the extended types
   *  set up for this Dialect before using a default association.
   */
  public SqlType<?> getSqlType(int sqlTypeId)
  { 
    SqlType<?> type=typeMap.get(sqlTypeId);
    if (type!=null)
    { return type;
    }
    else
    { return SqlType.getSqlType(sqlTypeId);
    }
  }
  
  /**
   * The default schema name to use for application tables if no specific
   *   schema name is specified.
   * 
   * @return
   */
  public String getDefaultSchemaName()
  { return null;
  }
}
