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
package spiralcraft.sql.model;

import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;
import spiralcraft.sql.ddl.CreateSchemaStatement;
import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.sql.Dialect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;

/**
 * A collection of SQL objects associated with a catalog in a database.
 */
public class Schema
{
  private static final ClassLog log=ClassLog.getInstance(Schema.class);
  private static final Level logLevel
    =ClassLog.getInitialDebugLevel(Schema.class,Level.INFO);
  
  private final ArrayList<Table> tables=new ArrayList<Table>();
  private final HashMap<String,Table> tableMap
    =new HashMap<String,Table>();

  private String catalogName;
  private String name;
  
  
  
  /**
   * Construct a Schema that can be used to compare or construct
   *   a database Schema
   *
   */
  public Schema()
  {
  }
  
  /**
   * Construct a Schema from the results of DatabaseMetaData.getSchemas()
   */
  public Schema(ResultSet rs)
    throws SQLException
  { 
    name=rs.getString(1);
    if (rs.getMetaData().getColumnCount()>1)
    { catalogName=rs.getString(2);
    }
    if (logLevel.isDebug())
    { log.debug("From DB: schema="+name+": catalog="+catalogName);
    }
  }
  
  public void readTables(Dialect dialect,DatabaseMetaData metadata)
    throws SQLException
  {
    ResultSet rs
      =metadata.getTables(catalogName,name!=null?name:"","%",new String[] {"TABLE","VIEW"});


    while (rs.next())
    { addTable(new Table(rs));
    }
    rs.close();
    
    for (Table table: tables)
    { table.readMetaData(dialect,metadata);
    }
  }
  
  
  public String getName()
  { return name;
  }
  
  public void setName(String name)
  { this.name=name;
  }
  
  public Table getTable(String tableName)
  { return tableMap.get(tableName);
  }
  
  public Table[] getTables()
  { return tables.toArray(new Table[tables.size()]);
  }
  
  public void addTable(Table table)
  { 
    tables.add(table);
    tableMap.put(table.getName(),table);
  }
  
  public List<DDLStatement> generateUpdateDDL(Dialect dialect,Schema storeVersion)
  {
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    if (storeVersion==null)
    {
      if (name!=null)
      { ret.add(new CreateSchemaStatement(name));
      }
      else
      { ret.add(new CreateSchemaStatement(dialect.getDefaultSchemaName()));
      }
    }
    
    for (Table table: tables)
    { 
      
      ret.addAll
        (table.generateUpdateDDL
            (dialect
            ,storeVersion==null
            ?null
            :storeVersion.getTable(table.getName())
            )
        );
    }
    return ret;
  }
  
}
