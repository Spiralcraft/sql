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

import spiralcraft.common.ContextualException;
import spiralcraft.data.Aggregate;
import spiralcraft.data.DataConsumer;
import spiralcraft.data.DataException;
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Tuple;
import spiralcraft.data.Type;
import spiralcraft.data.Field;
import spiralcraft.data.Key;

import spiralcraft.data.access.CursorAggregate;
import spiralcraft.data.access.EntityAccessor;
import spiralcraft.data.access.cache.EntityCache;
import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.EquiJoin;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Scan;
import spiralcraft.data.query.Selection;
import spiralcraft.data.sax.DataReader;
import spiralcraft.data.spi.ArrayDeltaTuple;

import spiralcraft.sql.data.ResultColumnMapping;
import spiralcraft.sql.data.ResultSetMapping;
import spiralcraft.sql.data.query.BoundEquiJoin;
import spiralcraft.sql.data.query.BoundScan;
import spiralcraft.sql.data.query.BoundSelection;
import spiralcraft.sql.dml.FromClause;
import spiralcraft.sql.dml.SelectList;
import spiralcraft.sql.dml.SelectListItem;
import spiralcraft.sql.dml.TableName;
import spiralcraft.sql.dml.WhereClause;
import spiralcraft.sql.dml.BooleanCondition;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;

import org.xml.sax.SAXException;

import spiralcraft.lang.Focus;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;

import spiralcraft.sql.model.Table;
import spiralcraft.sql.model.Column;
import spiralcraft.sql.model.KeyConstraint;

import spiralcraft.util.Path;
import spiralcraft.util.tree.LinkedTree;
import spiralcraft.vfs.Resource;


/**
 * An association between a Type and a Table in a SQL Store
 */
public class TableMapping
  implements EntityAccessor<Tuple>
{
  private static final ClassLog log
    =ClassLog.getInstance(TableMapping.class);
  private static Level logLevel
    =ClassLog.getInitialDebugLevel(TableMapping.class,Level.INFO);
  
  private Type<?> type;
  private String tableName;
  private String schemaName;
  private ArrayList<ColumnMapping> columnMappings
    =new ArrayList<ColumnMapping>();
  private Scan scan;
  private Table tableModel;
  private SqlUpdater updater;
  private LinkedTree<ColumnMapping> columnMappingTree;
  private SqlStore sqlStore;
  private boolean resolved;
  private volatile long lastTransactionId;
  private FromClause fromClause;
  private SelectList selectList;
  private ResultSetMapping resultSetMapping;
  private BoundScan boundScan;
  private Focus<?> focus;
  private EntityCache cache;

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
  { return new Type<?>[] {type};
  }
  
  @Override
  public boolean containsType(Type<?> type)
  { return type==this.type;
  }
  
  public void setStore(SqlStore store)
  { this.sqlStore=store;
  }
  
  public EntityCache getCache()
  { return cache;
  }
  
  @Override
  public Focus<?> bind(Focus<?> context)
    throws ContextualException
  { 
    this.focus=context;
    boundScan=new BoundScan(getScan(),focus,this.sqlStore,this);
    boundScan.resolve();    
    return context;
  }
  
  @Override
  public BoundQuery<?,Tuple> query(Query query,Focus<?> focus)
    throws DataException
  { 
    BoundQuery<?,Tuple> ret=solve(query,focus);
    if (ret==null)
    { ret= query.solve(focus,this);
    }
    ret.resolve();
    return ret;
  }
  
  @Override
  public BoundQuery<?,Tuple> solve(Query query,Focus<?> focus)
    throws DataException
  {
    if (logLevel.isFine())
    { log.fine("Solving "+query);
    }
    if (query.getSources().size()==1 
        && (query.getSources().get(0) instanceof Scan)
       )
    {
      if (query instanceof Selection)
      { 
        
        Query factor=query.factor();
        if (factor==null)
        {
          BoundSelection boundSelection
            =new BoundSelection((Selection) query,focus,this.sqlStore,this);
          if (logLevel.isFine())
          { 
            log.fine
              ("SqlStore.query: remainder="+boundSelection.getRemainderCriteria());
          }
          boundSelection.resolve();
          return boundSelection;
        }
        else if (factor instanceof EquiJoin)
        { 
          if (logLevel.isFine())
          { 
            log.fine("Factored selection "
              +((Selection) query).getConstraints().getText()+" to EquiJoin");
          }
          BoundEquiJoin boundEquiJoin
            =new BoundEquiJoin((EquiJoin) factor,focus,this.sqlStore,this);
          boundEquiJoin.resolve();
          return boundEquiJoin;
        }
        else
        { 
          if (logLevel.isFine())
          { 
            log.fine("Factored selection "
              +((Selection) query).getConstraints().getText()+" to "+factor);
          }
          factor.solve(focus,this);
        }
      }
      else if (query instanceof EquiJoin)
      {
        BoundEquiJoin boundEquiJoin
          =new BoundEquiJoin((EquiJoin) query,focus,this.sqlStore,this);
        boundEquiJoin.resolve();
        return boundEquiJoin;
      }
    }
    else if (query instanceof Scan)
    { return getBoundScan();
    } 
    if (logLevel.isFine())
    { log.fine("Couldn't solve "+query);
    }
    return null;
  }
  
  private BoundScan getBoundScan()
  { return boundScan;
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
    throws DataException
  {
    
    if (primaryKeyWhereClause==null)
    {
      if (type.getPrimaryKey()==null)
      { throw new DataException("No primary key for "+type.getURI());
      }
      
      BooleanCondition condition=null;
      for (Field<?> field:type.getPrimaryKey().fieldIterable())
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
  
  private ColumnMapping getMappingForField(String fieldName)
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
    { return boundScan;
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
  public SqlUpdater getUpdater()
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
  
  public ResultSetMapping getResultSetMapping()
  { return resultSetMapping;
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
  
  public void restore(Resource xmlData)
    throws DataException,IOException
  { 
    DataReader reader=new DataReader();
    final SqlUpdater updater=getUpdater();
    DataConsumer<? super Tuple> restoreConsumer
      =new DataConsumer<Tuple>()
    {

      @Override
      public void dataInitialize(
        FieldSet fieldSet)
        throws DataException
      { updater.dataInitialize(type.getFieldSet());
      }

      @Override
      public void dataAvailable(
        Tuple tuple)
        throws DataException
      { 
        DeltaTuple dt=new ArrayDeltaTuple(null,tuple);
        try
        { updater.dataAvailable(dt);
        }
        catch (Exception x)
        { log.warning("Error updating \r\n"+tuple+" : \r\n"+dt);
        }
      }

      @Override
      public void dataFinalize()
        throws DataException
      { updater.dataFinalize();
      }

      @Override
      public void setDebug(
        boolean debug)
      { 
      }
    };
    reader.setDataConsumer(restoreConsumer);
    
    try
    { reader.readFromResource(xmlData,Type.getAggregateType(type));
    }
    catch (SAXException e)
    { throw new DataException("XML Error reading data for table "+tableName,e);
    }
    
  }
  
  public Aggregate<Tuple> snapshot()
    throws DataException
  { return new CursorAggregate<Tuple>(getAll(type).execute());
  }
  
  public long getLastTransactionId()
  { return lastTransactionId;
  }
  
  public FromClause getFromClause()
  { return fromClause;
  }
  
  public SelectList getSelectList()
  { return selectList;
  }
  
  private ColumnMapping resolveMappingForField(Field<?> field)
  {
    ColumnMapping columnMapping=getMappingForField(field.getName());
    if (columnMapping==null)
    { 
      if (TypeManager.isPersistent(type,field))
      {
        columnMapping=new ColumnMapping();
        columnMapping.setStore(sqlStore);        
        columnMapping.setField(field);
        columnMapping.setPath(new Path().append(field.getName()));        
        columnMapping.resolve();
        
        // Check to make sure the type is resolvable
        Column[] models=columnMapping.getColumnModels();
        if (models.length>0)
        { addColumnMapping(columnMapping);
        }
        else
        { 
          columnMapping=null;
          log.warning
            ("Non transient field "+field.getURI()+" could not be mapped to "
            +" table columns and will not be persisted"
            ); 
        }
      }
    }
    else
    { 
      // Mapping has been manually specified
      columnMapping.setStore(sqlStore);
      columnMapping.setField(field);
      columnMapping.setPath(new Path().append(field.getName()));
      columnMapping.resolve();
      
    }
    

    // log.fine("Mapped "+field.getURI()+" to "+columnMapping);
    return columnMapping;
  }
  
  /**
   * Fill in missing details, publish interface
   */
  public void resolve()
    throws DataException
  { 
    if (resolved)
    { return;
    }
    
    if (logLevel.isDebug())
    { log.debug("Resolving mapping for "+type.getURI());
    }
    this.cache=new EntityCache(type);
    ArrayList<ColumnMapping> orderedColumns
      =new ArrayList<ColumnMapping>();
    
    HashSet<String> seen=new HashSet<String>();
    for (Field<?> field: type.getFieldSet().fieldIterable())
    {
      // Skip base type fields hidden by subtype fields
      if (seen.contains(field.getName()))
      { continue;
      }
      seen.add(field.getName());
      
      ColumnMapping columnMapping=resolveMappingForField(field);
      if (columnMapping!=null)
      { orderedColumns.add(columnMapping);
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
      // log.fine("Mapping "+mapping);
      for (Column column: mapping.getColumnModels())
      { 
        
        if (logLevel.isDebug())
        { log.fine(i+": "+column);
        }
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
          // Unmapped columns happen when a relative field is based on
          //   a non-persistent key field 
          continue;
          
        }
        
        Column[] fieldCols=mapping.getColumnModels();
          
        if (fieldCols!=null)
        {
          for (Column col: fieldCols)
          { keyCols.add(col);
          }
        }
      }
      
      if (keyCols.size()>0)
      {

        constraint.setColumns(keyCols.toArray(new Column[keyCols.size()]));
        constraint.setPrimary(key.isPrimary());
        constraint.setUnique(key.isUnique());

        tableModel.addKeyConstraint(constraint);
      }
      
      
    }
    tableNameSqlFragment=new TableName(schemaName,tableName);

    updater=new SqlUpdater(sqlStore,this);
    
    fromClause
      =new FromClause
        (getSchemaName()
        ,getTableName()
        );
    
    selectList=new SelectList();
    LinkedTree<ResultColumnMapping> foldTree=new LinkedTree<ResultColumnMapping>();
    int columnCount=0;
    generateSelectList(getColumnMappingTree(),selectList,foldTree,columnCount);
    resultSetMapping=new ResultSetMapping(type.getFieldSet(),foldTree);
    


    resolved=true;
  }
  
  private int generateSelectList
    (LinkedTree<ColumnMapping> columnMapping
    ,SelectList selectList
    ,LinkedTree<ResultColumnMapping> foldTree
    ,int columnCount
    )
  {
    for (LinkedTree<ColumnMapping> mapping : columnMapping)
    { columnCount=generateSelectListItem(mapping,selectList,foldTree,columnCount);
    }
    return columnCount;    
  }
  
  private int generateSelectListItem
    (LinkedTree<ColumnMapping> node
    ,SelectList selectList
    ,LinkedTree<ResultColumnMapping> foldTree
    ,int columnCount
    )
  {
    if (node.isLeaf())
    {
      SelectListItem selectListItem=node.get().getSelectListItem();
      // Single field
      if (selectListItem!=null)
      { 
        selectList.addItem(selectListItem);
        foldTree.addChild
          (new LinkedTree<ResultColumnMapping>
            (new ResultColumnMapping( (columnCount++)+1,node.get()))
            );
      }
      else
      { foldTree.addChild(new LinkedTree<ResultColumnMapping>());
      }
    }
    else
    { 
      LinkedTree<ResultColumnMapping> child=new LinkedTree<ResultColumnMapping>();
      foldTree.addChild(child);
      columnCount=generateSelectList(node,selectList,child,columnCount);
    }
    
    return columnCount;
  }
   
}
