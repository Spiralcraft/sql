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
package spiralcraft.sql.data.store;

import spiralcraft.data.Type;
import spiralcraft.data.access.Schema;

import java.util.ArrayList;

/**
 * An association between a database Schema name and a 
 *   spiralcraft.data.transport.Schema
 */
public class SchemaMapping
{
 
  private Schema schema;
  private String schemaName;
 
  public Schema getSchema()
  { return schema;
  }
 
  public void setSchema(Schema schema)
  { this.schema=schema;
  }
 
  public String getSchemaName()
  { return schemaName;
  }
 
  public void setSchemaName(String schemaName)
  { this.schemaName=schemaName;
  }
 

  /**
   * Create table mappings for all the types in the Schema
   */
  public TableMapping[] createTableMappings()
  {
    ArrayList<TableMapping> mappings
      =new ArrayList<TableMapping>();
   
    for (Type type: schema.getTypes())
    {
      TableMapping mapping=new TableMapping();
      mapping.setSchemaName(schemaName);
      mapping.setType(type);
      mapping.setTableName
        (type.getPackageURI().relativize(type.getURI()).getPath());
      mappings.add(mapping);
    }
    return mappings.toArray(new TableMapping[mappings.size()]);
  }
}
