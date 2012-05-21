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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import spiralcraft.log.ClassLog;
import spiralcraft.sql.model.KeyConstraint;
import spiralcraft.sql.model.Table;

/**
 * Resolves language features that may differ between database vendors. Methods in this
 *   class may be overridden by database specific methods.
 */
public class Dialect
{
  
  protected final ClassLog log
    =ClassLog.getInstance(getClass());
  
  public static final Dialect DEFAULT
    =new Dialect();
  
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
  
  public String getQualifiedTableName(String catalog,String schema,String table)
  { 
    return (catalog!=null?("\""+catalog+"\""+"."):"")
           +(schema!=null?("\""+schema+"\""+"."):"")
           +"\""+table+"\"";
  }
  
  public KeyConstraint[] getUniqueConstraints
    (Connection connection,Table table)
      throws SQLException
  { 
    ArrayList<KeyConstraint> constraints=new ArrayList<KeyConstraint>();
    ResultSet c=connection.createStatement().executeQuery
        ("SELECT A.constraint_name"
        +" from information_schema.table_constraints A"
        +"  where "
        +(table.getCatalogName()!=null?"A.table_catalog='"+table.getCatalogName()+"' AND ":"")
        +(table.getSchemaName()!=null?"A.table_schema='"+table.getSchemaName()+"' AND ":"")
        +(table.getName()!=null?"A.table_name='"+table.getName()+"' AND ":"")
        +"A.constraint_type='UNIQUE'"
      );
    while (c.next())
    { 
      String constraintName=c.getString(1);
      ResultSet rs=connection.createStatement().executeQuery
        ("SELECT A.table_catalog,A.table_schema,A.table_name,A.column_name,A.ordinal_position"
        +" from information_schema.key_column_usage A"
        +"  where "
        +(table.getCatalogName()!=null?"A.table_catalog='"+table.getCatalogName()+"' AND ":"")
        +(table.getSchemaName()!=null?"A.table_schema='"+table.getSchemaName()+"' AND ":"")
        +(table.getName()!=null?"A.table_name='"+table.getName()+"' AND ":"")
        +"A.constraint_name='"+constraintName+"'"
        );
      constraints.add(new KeyConstraint(table,rs,false));
    }
    return constraints.toArray(new KeyConstraint[constraints.size()]);
  }  
  
}
