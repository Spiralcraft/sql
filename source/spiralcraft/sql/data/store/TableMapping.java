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

import spiralcraft.data.Aggregate;
import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;
import spiralcraft.data.Type;
import spiralcraft.data.Field;
import spiralcraft.data.Key;

import spiralcraft.data.access.CursorAggregate;
import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Queryable;
import spiralcraft.data.query.Scan;

import spiralcraft.sql.data.query.BoundScan;
import spiralcraft.sql.dml.TableName;
import spiralcraft.sql.dml.WhereClause;
import spiralcraft.sql.dml.BooleanCondition;

import java.util.HashMap;
import java.util.ArrayList;

import spiralcraft.lang.Focus;

import spiralcraft.sql.model.Table;
import spiralcraft.sql.model.Column;
import spiralcraft.sql.model.KeyConstraint;

import spiralcraft.util.Path;
import spiralcraft.util.tree.LinkedTree;


/**
 * An association between a Type and a Table in a SQL Store
 */
public class TableMapping
  implements Queryable<Tuple>
{
  private Type<?> type;
  private String tableName;
  private String schemaName;
  private ArrayList<ColumnMapping> columnMappings
    =new ArrayList<ColumnMapping>();
  private Scan scan;
  private Table tableModel;
  private Updater updater;
  private LinkedTree<ColumnMapping> columnMappingTree;
  private SqlStore sqlStore;
  private boolean resolved;
  private volatile long lastTransactionId;


  private HashMap<String,ColumnMapping> columnFieldMap
    =new HashMap<String,ColumnMapping>();
  
  private HashMap<String,ColumnMapping> columnNameMap
    =new HashMap<String,ColumnMapping>();
  
  private HashMap<Path,ColumnMapping> columnPathMap
    =new HashMap<Path,ColumnMapping>();

  private WhereClause primaryKeyWhereClause;
  private TableName tableNameSqlFragment;
  
  public Type<?> getType()
  { return type;
  }
  
  @Override
  public Type<?>[] getTypes()
  { return new Type[] {type};
  }
  
  @Override
  public boolean containsType(Type<?> type)
  { return type==this.type;
  }
  
  public void setStore(SqlStore store)
  { this.sqlStore=store;
  }
  
  @Override
  public BoundQuery<?,Tuple> query(Query query,Focus<?> focus)
    throws DataException
  { return query.solve(focus,this);
  }
  
  public void setType(Type<?> type)
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
    if (resolved)
    { 
      throw new IllegalStateException
        ("Mappings cannot be changed after startup");
    }
    
    for (ColumnMapping mapping: fieldMappings)
    { addColumnMapping(mapping);
    }
    
  }

  public WhereClause getPrimaryKeyWhereClause()
  {
    if (primaryKeyWhereClause==null)
    {
      BooleanCondition condition=null;
      for (Field<?> field:type.getScheme().getPrimaryKey().fieldIterable())
      {
        ColumnMapping mapping=getMappingForField(field.getName());
        if (condition==null)
        { condition=mapping.getParameterizedKeyCondition();
        }
        else
        { condition=condition.and(mapping.getParameterizedKeyCondition());
        }
      }
      primaryKeyWhereClause=new WhereClause(condition);
      
    }
    return primaryKeyWhereClause;
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

  public ColumnMapping getMappingForPath(Path path)
  { return columnPathMap.get(path);
  }
  
  private void generateFlatPerspective(Path path,ArrayList<ColumnMapping> columns)
  {
    for (ColumnMapping columnMapping: columns)
    {
      Path subPath=path.append(columnMapping.getFieldName());
      columnPathMap.put(subPath, columnMapping);
//      System.err.println
//        ("TableMapping: mapping flattened column '"
//        +columnMapping.getColumnName()+"' for "+subPath
//        );
      if (columnMapping.isFlattened())
      { generateFlatPerspective(subPath,columnMapping.getChildMappings());
      }

      if (columnMapping.getColumnName()!=null)
      { columnNameMap.put(columnMapping.getColumnName(), columnMapping);
      }
    }
  }
  
  public ColumnMapping getMappingForField(String fieldName)
  { return columnFieldMap.get(fieldName);
  }

  public ColumnMapping getMappingForColumn(String columnName)
  { return columnNameMap.get(columnName);
  }

  @Override
  public BoundScan getAll(Type<?> type)
    throws DataException
  {
    if (this.type==type)
    { return new BoundScan(getScan(),null,sqlStore);
    }
    else
    { return null;
    }
  }
  
  public synchronized Scan getScan()
  { 
    if (scan==null && type!=null)
    { this.scan=new Scan(type);
    }
    return this.scan;
  }
  
  /**
   * @return The Updater which handles SQL update logic for this table
   */
  public Updater getUpdater()
  { return updater;
  }
  
  public TableName getTableNameSqlFragment()
  { return tableNameSqlFragment;
  }
  
  /**
   * 
   * @return The tree of ColumnMappings that maps this Scheme to a SQL Table
   * 
   */
  public LinkedTree<ColumnMapping> getColumnMappingTree()
  { 
    if (columnMappingTree==null)
    {
      columnMappingTree=new LinkedTree<ColumnMapping>();
      for (ColumnMapping mapping: columnMappings)
      { columnMappingTree.addChild(mapping.getColumnMappings());
      }
    }
    return columnMappingTree;
  }
  
  public LinkedTree<ColumnMapping> getColumnMappings(Field<?> field)
  {
    
    ColumnMapping columnMapping
      =getMappingForField(field.getName());
    if (columnMapping!=null)
    { return columnMapping.getColumnMappings();
    }
    else
    { return null;
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
   * Add a ColumnMapping for a Field
   */
  private void addColumnMapping(ColumnMapping mapping)
  { 
    columnMappings.add(mapping);
    columnFieldMap.put(mapping.getFieldName(),mapping);
  }
  
  /**
   * Update the table to match the presented dataset
   * 
   * @param aggregate
   */
  public void update(Aggregate<Tuple> aggregate)
    throws DataException
  {
  }
  
  public Aggregate<Tuple> snapshot()
    throws DataException
  { return new CursorAggregate<Tuple>(getAll(type).execute());
  }
  
  public long getLastTransactionId()
  { return lastTransactionId;
  }
  
  /**
   * Fill in missing details, publish interface
   */
  public void resolve()
  { 
    
    ArrayList<ColumnMapping> orderedColumns
      =new ArrayList<ColumnMapping>();
    
    for (Field<?> field: type.getScheme().fieldIterable())
    {
      ColumnMapping columnMapping=getMappingForField(field.getName());
      if (columnMapping==null)
      { 
        if (!field.isTransient())
        {
          columnMapping=new ColumnMapping();
          columnMapping.setField(field);
          addColumnMapping(columnMapping);
        }
      }
      else
      { columnMapping.setField(field);
      }
        
      if (columnMapping!=null)
      {
        columnMapping.setPath(new Path().append(field.getName()));
        orderedColumns.add(columnMapping);
        columnMapping.setStore(sqlStore);
        columnMapping.resolve();
      }
      
    }
    columnMappings.clear();
    columnMappings.addAll(orderedColumns);
    
    generateFlatPerspective(new Path(),columnMappings);
    
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

    for (Key<?> key: type.getScheme().keyIterable())
    { 
      KeyConstraint constraint=new KeyConstraint();
      ArrayList<Column> keyCols=new ArrayList<Column>();
      for (Field<?> field:key.fieldIterable())
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
    tableNameSqlFragment=new TableName(schemaName,tableName);

    updater=new Updater(sqlStore,this);
    resolved=true;
  }
}
