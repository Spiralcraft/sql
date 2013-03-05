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
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.Field;
import spiralcraft.data.FieldSet;


import spiralcraft.data.lang.DataReflector;

import spiralcraft.data.access.Updater;

import spiralcraft.lang.BindException;
import spiralcraft.lang.Focus;
import spiralcraft.lang.reflect.BeanReflector;
import spiralcraft.lang.spi.ThreadLocalChannel;
import spiralcraft.sql.dml.InsertStatement;
import spiralcraft.sql.dml.DeleteStatement;
import spiralcraft.sql.dml.UpdateStatement;
import spiralcraft.sql.dml.TableValueConstructor;
import spiralcraft.sql.dml.RowValueConstructorElements;

import spiralcraft.sql.SqlFragment;

import spiralcraft.util.Path;


import java.util.HashMap;

import java.util.ArrayList;

/**
 * Handles SQL table updates by primary key
 */
public class SqlUpdater
  extends Updater<DeltaTuple>
{

  private TableMapping tableMapping;
  private SqlStore store;

  private HashMap<ArrayList<Path>,UpdateStatement> updateStatements
    =new HashMap<ArrayList<Path>,UpdateStatement>();
  
  private HashMap<ArrayList<Path>,InsertStatement> insertStatements
    =new HashMap<ArrayList<Path>,InsertStatement>();
  
  private DeleteStatement deleteStatement;
  

  private ThreadLocalChannel<Batch> batch;
  private ThreadLocalChannel<DeltaTuple> tuple;
  private Focus<DeltaTuple> paramFocus;

  private HashMap<SqlFragment,BoundUpdateStatement> boundStatements
    =new HashMap<SqlFragment,BoundUpdateStatement>();
  
  public SqlUpdater(SqlStore store,TableMapping tableMapping)
  { 
    this.tableMapping=tableMapping;
    this.store=store;
    setFieldSet(tableMapping.getType().getFieldSet());
  }


  
  /**
   * Retrieve from the cache or build an INSERT statement for a particular combination
   *   of dirty fields in the specified DeltaTuple
   */
  synchronized InsertStatement getInsertStatement(DeltaTuple tuple)
  {
    ArrayList<Path> paths=new ArrayList<Path>();
    buildDirtyPaths(tuple,paths,new Path());
    if (paths.size()==0)
    { return null;
    }
    
    InsertStatement insertStatement=insertStatements.get(paths);
    if (insertStatement==null)
    {
      insertStatement=new InsertStatement();
      insertStatement.setTableName(tableMapping.getTableNameSqlFragment());
      
      RowValueConstructorElements elements
        =new RowValueConstructorElements();
      
      for (Path path: paths)
      {
        ColumnMapping mapping=tableMapping.getMappingForPath(path);
        insertStatement.addColumnName("\""+mapping.getColumnName()+"\"");
        elements.addItem(mapping.getParameterReference());
      }
      
      insertStatement.setQueryExpression(new TableValueConstructor(elements));
      insertStatements.put(paths,insertStatement);
    }
    return insertStatement;
  }
  
  
  /**
   * Retrieve from the cache or build an UPDATE statement for a particular combination
   *   of dirty fields in the specified DeltaTuple
   */
  synchronized UpdateStatement getUpdateStatement(DeltaTuple tuple)
    throws DataException
  {
    ArrayList<Path> paths=new ArrayList<Path>();
    buildDirtyPaths(tuple,paths,new Path());
    if (paths.size()==0)
    { return null;
    }
    
    UpdateStatement updateStatement=updateStatements.get(paths);
    if (updateStatement==null)
    {
      updateStatement=new UpdateStatement();
      updateStatement.setTableName(tableMapping.getTableNameSqlFragment());
      for (Path path: paths)
      {
        ColumnMapping mapping=tableMapping.getMappingForPath(path);
        updateStatement.addSetClause(mapping.getParameterizedSetClause());
      }
      updateStatement.setWhereClause(tableMapping.getPrimaryKeyWhereClause());
      updateStatements.put(paths,updateStatement);
    }
    return updateStatement;
    
  }
  
  /**
   * Retrieve or build a DELETE statement for this Type/Table
   */
  synchronized DeleteStatement getDeleteStatement()
    throws DataException
  { 
    if (deleteStatement==null)
    {
      deleteStatement=new DeleteStatement();
      deleteStatement.setTableName(tableMapping.getTableNameSqlFragment());
      deleteStatement.setWhereClause(tableMapping.getPrimaryKeyWhereClause());
    }
    return deleteStatement;

  }
  
  private void buildDirtyPaths(DeltaTuple tuple,ArrayList<Path> paths,Path parentPath)
  {
    for (Field<?> field: tuple.getDirtyFields())
    {
      
      Path subPath=parentPath.append(field.getName());
      ColumnMapping mapping
        =tableMapping.getMappingForPath(subPath);
      if (mapping!=null)
      {
        if (mapping.isFlattened())
        {
          DeltaTuple subDelta=null;
          try
          { 
            subDelta
              =(DeltaTuple) field.getValue(tuple);
          }
          catch (DataException x)
          { 
            x.printStackTrace();
            continue;
          }
          if (subDelta.isDirty())
          { buildDirtyPaths(subDelta,paths,subPath);
          }
        }
        else
        { paths.add(subPath);
        }
      }
      else
      { 
        if (debug)
        { log.info("Field "+field.getURI()+" is dirty but has no mapping exists fpr "+subPath);
        }
      }
    }
    
  }


  @Override
  public Focus<?> bind(Focus<?> focus)
    throws BindException
  {

    batch
      =new ThreadLocalChannel<Batch>
        (BeanReflector.<Batch>getInstance(Batch.class));
    
    tuple
      =new ThreadLocalChannel<DeltaTuple>
        (DataReflector.<DeltaTuple>getInstance(tableMapping.getType()));
    paramFocus=focus.chain(tuple);
    return super.bind(focus);
  }

  @Override
  public void dataInitialize(FieldSet fieldSet) 
    throws DataException
  {
    batch.push(new Batch());
    tuple.push();
    super.dataInitialize(fieldSet);

  }
  
  @Override
  public void dataFinalize() throws DataException
  {
    
    try
    {
      super.dataFinalize();
      batch.get().flush();
    }
    finally
    { batch.pop();
    }
  }
  
  
  @Override
  public void dataAvailable(DeltaTuple deltaTuple)
    throws DataException
  {
    super.dataAvailable(deltaTuple);
    tuple.set(deltaTuple);
    if (!deltaTuple.isDirty())
    { return;
    }
    if (deltaTuple.getOriginal()==null)
    { batch.get().execute(getInsertStatement(deltaTuple));
    }
    else if (deltaTuple.isDelete())
    { batch.get().execute(getDeleteStatement());
    }
    else
    { batch.get().execute(getUpdateStatement(deltaTuple));
    }
    
  }
  

  
  
  
  class Batch
    extends spiralcraft.data.access.Updater<DeltaTuple>
  {

    private SqlFragment lastOp;
    private BoundUpdateStatement currentStatement;
    


    
    private void flush()
      throws DataException
    {
      
    }

    private void execute(SqlFragment statement)
      throws DataException
    { 
      if (statement==null)
      { return;
      }
      if (lastOp!=statement)
      {
        lastOp=statement;
        currentStatement=boundStatements.get(statement);
        if (currentStatement==null)
        {
          currentStatement
            =new BoundUpdateStatement(store,tableMapping.getType().getScheme());
          currentStatement.setSqlFragment(statement);
          currentStatement.bindParameters(paramFocus);
          boundStatements.put(statement,currentStatement);
        }
      }
      currentStatement.execute();
    }

  }
}
