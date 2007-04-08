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

import spiralcraft.data.Type;
import spiralcraft.data.Field;
import spiralcraft.data.Key;

import spiralcraft.data.query.Scan;

import spiralcraft.sql.dml.DerivedColumn;
import spiralcraft.sql.dml.IdentifierChain;
import spiralcraft.sql.dml.SelectListItem;

import java.util.HashMap;
import java.util.ArrayList;

import spiralcraft.registry.RegistryNode;
import spiralcraft.registry.Registrant;

import spiralcraft.sql.model.Table;
import spiralcraft.sql.model.Column;
import spiralcraft.sql.model.KeyConstraint;


/**
 * An association between a Type and a Table in a SQL Store
 */
public class TableMapping
  implements Registrant
{
  private Type type;
  private String tableName;
  private String schemaName;
  private ArrayList<ColumnMapping> columnMappings
    =new ArrayList<ColumnMapping>();
  private Scan Scan;
  private RegistryNode registryNode;
  private Table tableModel;


  private HashMap<String,ColumnMapping> columnFieldMap
    =new HashMap<String,ColumnMapping>();
  
  private HashMap<String,ColumnMapping> columnNameMap
    =new HashMap<String,ColumnMapping>();

  public Type getType()
  { return type;
  }
  
  public void setType(Type type)
  { this.type=type;
  }
  
  public String getTableName()
  { return tableName;
  }
  
  public String getQualifiedTableName()
  {
    if (schemaName!=null)
    { return schemaName+"."+tableName;
    }
    else
    { return tableName;
    }
  }
  
  public void setTableName(String tableName)
  { this.tableName=tableName;
  }
  
  public String getSchemaName()
  { return schemaName;
  }
  
  public void setSchemaName(String schemaName)
  { this.schemaName=schemaName;
  }
  
  public void setColumnMappings(ColumnMapping[] fieldMappings)
  { 
    if (registryNode!=null)
    { 
      throw new IllegalStateException
        ("Mappings cannot be changed after startup");
    }
    
    for (ColumnMapping mapping: fieldMappings)
    { addColumnMapping(mapping);
    }
    
  }

  public ColumnMapping[] getColumnMappings()
  { 
    if (columnMappings!=null)
    { return columnMappings.toArray(new ColumnMapping[columnMappings.size()]);
    }
    else
    { return null;
    }
  }
  
  public ColumnMapping getMappingForField(String fieldName)
  { return columnFieldMap.get(fieldName);
  }

  public ColumnMapping getMappingForColumn(String columnName)
  { return columnNameMap.get(columnName);
  }

  public synchronized Scan getScan()
  { 
    if (Scan==null && type!=null)
    { 
      Scan query=new Scan();
      query.setType(type);
      this.Scan=query;
    }
    return this.Scan;
  }
  
  public SelectListItem[] createSelectListItems(Field field)
  {
    ColumnMapping columnMapping
      =getMappingForField(field.getName());
    if (columnMapping!=null)
    { return columnMapping.createSelectListItems();
    }
    else
    { return new SelectListItem[] {new DerivedColumn(new IdentifierChain(field.getName()))};
    }

  }
  
  public Table getTableModel()
  { return tableModel;
  }
  
  /**
   * Update critical unspecified values with defaults that are preset when 
   *   overriding a default table mapping.
   */
  void copyDefaults(TableMapping defaultMapping)
  { 
    if (tableName==null)
    { tableName=defaultMapping.getTableName();
    }
    if (schemaName==null)
    { schemaName=defaultMapping.getSchemaName();
    }
  }
  
  /**
   * 
   */
  private void addColumnMapping(ColumnMapping mapping)
  { 
    columnMappings.add(mapping);
    columnNameMap.put(mapping.getColumnName(),mapping);
    columnFieldMap.put(mapping.getFieldName(),mapping);
  }
  
  /**
   * Fill in missing details, publish interface
   */
  public void register(RegistryNode node)
  { 
    registryNode=node;
    node=node.createChild(getSchemaName()+"."+getQualifiedTableName());
    node.registerInstance(TableMapping.class, this);
    
    ArrayList<ColumnMapping> orderedColumns
      =new ArrayList<ColumnMapping>();
    
    for (Field field: type.getScheme().fieldIterable())
    {
      ColumnMapping columnMapping=getMappingForField(field.getName());
      if (columnMapping==null)
      { 
        columnMapping=new ColumnMapping();
        columnMapping.setField(field);
        addColumnMapping(columnMapping);
      }
      else
      { columnMapping.setField(field);
      }
      orderedColumns.add(columnMapping);
      columnMapping.register(node);
    }
    columnMappings.clear();
    columnMappings.addAll(orderedColumns);
    
    
    tableModel=new Table();
    tableModel.setName(tableName);
    tableModel.setSchemaName(schemaName);
    int i=1;
    for (ColumnMapping mapping: columnMappings)
    { 
      for (Column column: mapping.getColumnModels())
      { 
        column.setPosition(i++);
        tableModel.addColumn(column);
      }
    }

    for (Key key: type.getScheme().keyIterable())
    { 
      KeyConstraint constraint=new KeyConstraint();
      ArrayList<Column> keyCols=new ArrayList<Column>();
      for (Field field:key.fieldIterable())
      { 
        ColumnMapping mapping=getMappingForField(field.getName());
        if (mapping==null)
        {
          System.err.println
          ("TableMapping: No column in key field '"+field.getName()+"'");
        }
        
        Column[] fieldCols=mapping.getColumnModels();
          
        if (fieldCols!=null)
        {
          for (Column col: fieldCols)
          { keyCols.add(col);
          }
        }
      }

      constraint.setColumns(keyCols.toArray(new Column[keyCols.size()]));
      constraint.setPrimary(key.isPrimary());
      constraint.setUnique(key.isUnique());

      tableModel.addKeyConstraint(constraint);
      
    }
    
  }
}
