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


import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.sql.Dialect;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/**
 * A collection of SQL objects associated with a catalog in a database.
 */
public class MetaData
{

  private final ArrayList<Schema> schemas=new ArrayList<Schema>();
  
  private final HashMap<String,Schema> schemaMap
    =new HashMap<String,Schema>();
  
  /**
   * Construct a MetaData collection that can be used to compare or construct
   *   a database.
   *
   */
  public MetaData()
  {
    
  }
  
  public Schema getSchema(String name)
  { return schemaMap.get(name);
  }
  
  public void addSchema(Schema schema)
  { 
    schemas.add(schema);
    schemaMap.put(schema.getName(), schema);
  }
  
 
  /**
   * Construct a MetaData collection that describes an existing database.
   */
  public MetaData(Dialect dialect,DatabaseMetaData metadata)
    throws SQLException
  {

    ResultSet rs=metadata.getSchemas();
    while (rs.next())
    { addSchema(new Schema(rs));
    }
     
    for (Schema schema: schemas)
    { schema.readTables(dialect,metadata);
    }
  } 

  public List<DDLStatement> generateUpdateDDL(Dialect dialect,MetaData storeVersion)
  { 
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    for (Schema schema: schemas)
    { 
      String schemaName=schema.getName();
      if (schemaName==null)
      { schemaName=dialect.getDefaultSchemaName();
      }
      
      Schema storeSchema
        =schema.getName()!=null
        ?storeVersion.getSchema(schema.getName())
        :storeVersion.getSchema(dialect.getDefaultSchemaName())
        ;
        
      ret.addAll(schema.generateUpdateDDL(dialect,storeSchema));
    }
    return ret;
  }
  
}
