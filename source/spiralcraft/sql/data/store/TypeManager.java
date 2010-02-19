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
package spiralcraft.sql.data.store;


import spiralcraft.data.DataException;
import spiralcraft.data.Type;

import spiralcraft.log.ClassLog;

import spiralcraft.sql.model.MetaData;
import spiralcraft.sql.model.Schema;
import spiralcraft.sql.model.Table;

import spiralcraft.sql.data.mappers.TypeMapper;
import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.sql.Dialect;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;



/**
 * Handles the translation from spiralcraft.data.Types into SQL tables and types
 */
public class TypeManager
{
  private static final ClassLog log
    =ClassLog.getInstance(TypeManager.class);
  
  private TypeMapper<?>[] typeMappers;
  
  private HashMap<Class<?>,TypeMapper<?>> typeMappersByTypeClass
    =new HashMap<Class<?>,TypeMapper<?>>();

  
  private SchemaMapping[] schemaMappings;

  private ArrayList<TableMapping> tableMappings
    =new ArrayList<TableMapping>();
  private TableMapping[] configuredTableMappings;
  private HashMap<Type<?>,TableMapping> tableMappingsByType
    =new HashMap<Type<?>,TableMapping>();
  
  private SqlStore store;
  
  private MetaData storeMetaData;
  private MetaData localMetaData;
  
  private Dialect dialect;
  
  private String schemaName;
  
  private boolean autoUpgrade;
  
  public SchemaMapping[] getSchemaMappings()
  { return schemaMappings;
  }

  public void setSchemaMappings(SchemaMapping[] schemaMappings)
  { this.schemaMappings=schemaMappings;
  }

  public TableMapping[] getTableMappings()
  { return tableMappings.toArray(new TableMapping[tableMappings.size()]);
  }
  
  public void setTableMappings(TableMapping[] tableMappings)
  { this.configuredTableMappings=tableMappings;
  }

  /**
   * Add a TableMapping, during post-configuration stage.
   * 
   * @param tableMapping
   */
  void addTableMapping(TableMapping tableMapping)
  {
    if (tableMapping.getSchemaName()==null)
    { tableMapping.setSchemaName(schemaName);
    }
    tableMappings.add(tableMapping);
    tableMappingsByType.put(tableMapping.getType(),tableMapping);
  }
  
  public void setTypeMappers(TypeMapper<?>[] typeMappers)
  { this.typeMappers=typeMappers;
  }

  /**
   * Specify the Dialect for the specific database server product that will provide
   *   product specific information and type translations.
   */
  public void setDialect(Dialect dialect)
  { this.dialect=dialect;
  }
  
  /**
   * Specify whether the database schema on the database server will be 
   *   automatically upgraded if it is not in synch with the packaged data
   *   definitions. 
   */
  public void setAutoUpgrade(boolean autoUpgrade)
  { this.autoUpgrade=autoUpgrade;
  }
  /**
   * Called by the SQL store to complete all unspecified mapping details
   *   and review metadata.
   */
  public void resolve()
  { 
    if (dialect==null)
    { dialect=new Dialect();
    }
    
    if (schemaName==null)
    { schemaName=dialect.getDefaultSchemaName();
    }
    
    resolveLocalDataModel();


    for (TableMapping mapping: tableMappings)
    { 
      mapping.setStore(store);
      mapping.resolve();
    }
    
  }
  
  void setStore(SqlStore store)
  { this.store=store;
  }
  
  private void resolveLocalDataModel()
  {
    for (TypeMapper<?> mapper: TypeMapper.getStandardTypeMappers())
    { 
      mapper.setDialect(dialect);
      typeMappersByTypeClass.put(mapper.getTypeClass(), mapper);
    }
      
    if (typeMappers!=null)
    {
      for (TypeMapper<?> mapper: typeMappers)
      { 
        mapper.setDialect(dialect);
        typeMappersByTypeClass.put(mapper.getTypeClass(), mapper);
      }
    }
    
    
    if (schemaMappings!=null)
    {
      for (SchemaMapping mapping: schemaMappings)
      {
        for (TableMapping tableMapping: mapping.createTableMappings())
        { 
          tableMappings.add(tableMapping);
          tableMappingsByType.put(tableMapping.getType(),tableMapping);
        }
      }
    }
    
    
    if (configuredTableMappings!=null)
    {
      for (TableMapping mapping: configuredTableMappings)
      { 
        TableMapping oldMapping
          =tableMappingsByType.remove(mapping.getType());
        
        if (oldMapping!=null)
        {  
          mapping.copyDefaults(oldMapping);
          tableMappings.remove(oldMapping);
          
        }
        tableMappings.add(mapping);
        tableMappingsByType.put(mapping.getType(),mapping);
      }
    }
    
  }
  
  /**
   * @return The TableMapping for the specified Type. If no TableMapping has been
   *   configured, a default TableMapping will be created if the Type's package is being
   *   handled by this store.
   */
  public TableMapping getTableMapping(Type<?> type)
  { return tableMappingsByType.get(type);
  }
  
  /**
   * @return the TypeMapper associated with a given Type.
   */
  public TypeMapper<?> getTypeMapperForType(Type<?> type)
  { 
    TypeMapper<?> mapper=null;
    Class<?> clazz=type.getClass();
    while (mapper==null && clazz!=null)
    { 
      mapper=typeMappersByTypeClass.get(clazz);
      clazz=clazz.getSuperclass();
    }
    return mapper;
  }
  
  /**
   * Read metadata from the db and update the local copy
   * 
   * @throws DataException
   */
  public void updateMetaData()
    throws DataException
  {
    try
    { 
      storeMetaData
        =new MetaData(store.getContextConnection().getMetaData());
    }
    catch (SQLException x)
    { throw new DataException("Error reading metadata: "+x,x);
    }
    
    
    localMetaData=new MetaData();
    
    if (schemaMappings!=null)
    {
      for (SchemaMapping mapping: schemaMappings)
      {
        Schema schema=new Schema();
        schema.setName(mapping.getSchemaName());
        localMetaData.addSchema(schema);
      }
    }
    
    for (TableMapping mapping: tableMappings)
    {
      Table table=mapping.getTableModel();
      log.fine("Adding table mapping "+table.getName());
      
      String schemaName=table.getSchemaName();
      if (schemaName!=null)
      { 
        Schema schema=localMetaData.getSchema(schemaName);
        if (schema!=null)
        { schema.addTable(table);
        }

      }
      else
      { 
        Schema schema=localMetaData.getSchema(dialect.getDefaultSchemaName());
        if (schema!=null)
        { schema.addTable(table);
        }
        else
        { 
          throw new DataException
            ("TypeManager: Mapping of "+mapping.getType().getURI()
            +" to table '"+table.getName()+"' no schemaName provided and no "
            +" default Schema in Dialect "
            );          
        }
      }
    }
      
  }
  
  private List<DDLStatement> generateUpdateDDL()
  { return localMetaData.generateUpdateDDL(dialect,storeMetaData);
  }
  
  public void ensureDataVersion()
    throws DataException
  {
    List<DDLStatement> upgrades=generateUpdateDDL();
    if (upgrades.size()>0)
    { 
      for (DDLStatement statement: upgrades)
      { 
        StringBuilder buff=new StringBuilder();
        statement.write(buff,"", null);
        System.err.println(buff.toString());
      }
      if (!autoUpgrade)
      {
        throw new DataException
          ("TypeManager: Schema out of synch- update is required");
      }
      else
      { store.executeDDL(upgrades);
      }
    }
    
  }
  

}
