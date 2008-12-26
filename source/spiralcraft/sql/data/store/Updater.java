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

import spiralcraft.data.access.DataConsumer;

import spiralcraft.data.lang.TupleFocus;

import spiralcraft.lang.Focus;
import spiralcraft.sql.dml.InsertStatement;
import spiralcraft.sql.dml.DeleteStatement;
import spiralcraft.sql.dml.UpdateStatement;
import spiralcraft.sql.dml.TableValueConstructor;
import spiralcraft.sql.dml.RowValueConstructorElements;

import spiralcraft.sql.SqlFragment;

import spiralcraft.util.Path;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.HashMap;

import java.util.ArrayList;

/**
 * Handles SQL table updates by primary key
 */
public class Updater
{

  private TableMapping tableMapping;
  private SqlStore store;

  private HashMap<ArrayList<Path>,UpdateStatement> updateStatements
    =new HashMap<ArrayList<Path>,UpdateStatement>();
  
  private HashMap<ArrayList<Path>,InsertStatement> insertStatements
    =new HashMap<ArrayList<Path>,InsertStatement>();
  
  private DeleteStatement deleteStatement;
  
  public Updater(SqlStore store,TableMapping tableMapping)
  { 
    this.tableMapping=tableMapping;
    this.store=store;
  }

  /**
   * <P>Batch-update data in the Store with the specified changes, expressed as 
   *   a SerialCursor of DeltaTuples of the same type.
   *   
   * <P>Any rejected updates will be sent to the specified DataConsumer.
   *   
   */
  public DataConsumer<DeltaTuple> newBatch(Focus<?> focus)
  { return new Batch(focus);
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
    
  }

  
  class Batch
    extends spiralcraft.data.access.Updater<DeltaTuple>
  {

    private Connection connection;
    private TupleFocus<DeltaTuple> focus;
    private Focus<?> parentFocus;
    private SqlFragment lastOp;
    private HashMap<SqlFragment,BoundUpdateStatement> boundStatements
      =new HashMap<SqlFragment,BoundUpdateStatement>();
    private BoundUpdateStatement currentStatement;
    
    public Batch(Focus<?> context)
    { 
      super(context);
      parentFocus=context;
    }
    
    @Override
    public void dataInitialize(FieldSet fieldSet) 
      throws DataException
    {
      super.dataInitialize(fieldSet);
      this.focus=new TupleFocus<DeltaTuple>(parentFocus,fieldSet);
      this.connection=store.allocateConnection();
    }
   
    @Override
    public void dataAvailable(DeltaTuple tuple)
      throws DataException
    {
      super.dataAvailable(tuple);
      if (!tuple.isDirty())
      { return;
      }
      focus.setTuple(tuple);
      if (tuple.getOriginal()==null)
      { execute(getInsertStatement(tuple));
      }
      else if (tuple.isDelete())
      { execute(getDeleteStatement());
      }
      else
      { execute(getUpdateStatement(tuple));
      }
      
    }
    
    @Override
    public void dataFinalize() throws DataException
    {
      super.dataFinalize();
      try
      { connection.close();
      }
      catch (SQLException x)
      { throw new DataException("Error closing connection: "+x,x);
      }
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
          currentStatement.bindParameters(focus);
          boundStatements.put(statement,currentStatement);
        }
      }
      currentStatement.execute();
    }

  }
}
