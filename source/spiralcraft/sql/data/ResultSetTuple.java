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
package spiralcraft.sql.data;

import spiralcraft.data.spi.AbstractTuple;

import spiralcraft.data.DeltaTuple;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Field;
import spiralcraft.data.Scheme;
import spiralcraft.data.Tuple;
import spiralcraft.data.DataException;

import spiralcraft.util.tree.LinkedTree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * A Tuple that retrieves data from a ResultSet. If the ResultSet is advanced,
 *   the Tuple's data will change.
 */
public class ResultSetTuple
  extends AbstractTuple
  implements Tuple
{
  private ResultSet resultSet;
  private final ResultMapping[] map;
  @SuppressWarnings("rawtypes")
  private final ResultSetTuple[] subs;
  private int resultColumnCount;
  @SuppressWarnings("rawtypes")
  
  public ResultSetTuple(FieldSet fieldSet)
  { 
    super(fieldSet instanceof Scheme?fieldSet:fieldSet.getType().getScheme());
    map=new ResultMapping[fieldSet.getFieldCount()];
    subs=new ResultSetTuple[fieldSet.getFieldCount()];
    defaultMap();
    if (fieldSet.getType()!=null)
    {
      if (fieldSet.getType().getBaseType()!=null)
      { 
        FieldSet baseScheme=fieldSet.getType().getBaseType().getScheme();
        if (baseScheme!=null)
        { baseExtent=createBaseExtent(baseScheme);
        }
      }
    }
    
  }
  
  public ResultSetTuple(FieldSet fieldSet,LinkedTree<ResultMapping> foldTree)
  { 
    super(fieldSet instanceof Scheme?fieldSet:fieldSet.getType().getScheme());
    map=new ResultMapping[fieldSet.getFieldCount()];
    subs=new ResultSetTuple[fieldSet.getFieldCount()];
    if (fieldSet.getType()!=null)
    {
      if (fieldSet.getType().getBaseType()!=null)
      { 
        FieldSet baseScheme=fieldSet.getType().getBaseType().getScheme();
        if (baseScheme!=null)
        { baseExtent=createBaseExtent(baseScheme);
        }
      }
    }
    
    applyFoldTree(foldTree);
  }

  private void applyFoldTree(LinkedTree<ResultMapping> foldTree)
  {
    if (baseExtent!=null)
    { ((ResultSetTuple) baseExtent).applyFoldTree(foldTree);
    }
    
    if (foldTree!=null)
    {
      
      int baseColumnCount=(baseExtent!=null)
            ?((ResultSetTuple) baseExtent).getResultColumnCount():0;
      int offset=baseColumnCount;
      int i=0;
      Iterator<? extends Field<?>> fieldIterator=fieldSet.fieldIterable().iterator();
      for (LinkedTree<ResultMapping> node: foldTree)
      {        
        if (offset-->0)
        { 
          // log.fine("Skipping "+node.get());
          // Skip base extent nodes
          continue;
        }
        Field<?> field;
        if (!fieldIterator.hasNext())
        { break;
        }
        else
        { field=fieldIterator.next();
        }
        
        while (field.isTransient() && fieldIterator.hasNext())
        { field=fieldIterator.next();
        }
        if (field.isTransient())
        { break;
        }
        
        
        // log.fine("Mapping "+field+" to "+node.get());
        if (node.get()!=null)
        { 
          map[i]=node.get();
        }
        if (!node.isLeaf())
        { 
          subs[i]
            =new ResultSetTuple
              (field.getType().getScheme()
              ,node
              );
        }
        i++;
        
      }
      resultColumnCount=baseColumnCount+i;
    }
    else
    { defaultMap();
    }
  }

  int getResultColumnCount()
  { return resultColumnCount;
  }
  
  private void defaultMap()
  {
    for (Field<?> field: fieldSet.fieldIterable())
    { map[field.getIndex()]=new ResultMapping(field.getIndex()+1);
    }
    resultColumnCount=fieldSet.getFieldCount();
  }
  
  /**
   * Replace the result set with the specified result set. Used to compose a single
   *   data stream from multiple SQL sources of the same type
   */
  public void setResultSet(ResultSet resultSet)
  { 
    this.resultSet=resultSet;
    for (ResultSetTuple tuple : subs)
    { 
      if (tuple!=null)
      { tuple.setResultSet(resultSet);
      }
    }
    if (baseExtent!=null)
    { ((ResultSetTuple) baseExtent).setResultSet(resultSet);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Object get(int index)
    throws DataException
  {
    if (subs[index]!=null)
    { return subs[index];
    }
    else if (map[index]!=null && map[index].resultSetColumn>0)
    {
      try
      { 
        Object sqlValue=resultSet.getObject(map[index].resultSetColumn);
        if (map[index].converter!=null)
        { 
          try
          { return map[index].converter.fromSql(sqlValue);
          }
          catch (ClassCastException x)
          {
            throw new DataException
              ("Unexpected data type "
              +sqlValue.getClass().getName()
              +" ["+sqlValue+"] applied to "+map[index].converter
              +" for column "+map[index].resultSetColumn+" ("
              +resultSet.getMetaData().getColumnName(map[index].resultSetColumn)+")"
              );
          }
        }
        else
        { return sqlValue;
        }
      }
      catch (SQLException x)
      { throw new DataException("Error reading result set: "+x,x);
      }
    }
    else
    { return null;
    }

  }

  /**
   *@return true, because this Tuple is simply a view of the ResultSet which can
   *  be advanced at any time
   */
  @Override
  public boolean isMutable()
  { return true;
  }

  /**
   *@return true, because this Tuple is simply a view of the ResultSet which can
   *  be advanced at any time
   */
  @Override
  public boolean isVolatile()
  { return true;
  }
  
  @Override
  protected AbstractTuple createBaseExtent(FieldSet fieldSet)
  { 
    // log.fine(fieldSet.toString());
    return new ResultSetTuple(fieldSet);
  }

  @Override
  protected AbstractTuple createBaseExtent(Tuple tuple) throws DataException
  { throw new UnsupportedOperationException("A ResultSetTuple cannot be written to");
  }

  @Override
  protected AbstractTuple createDeltaBaseExtent(DeltaTuple tuple) 
      throws DataException
  { throw new UnsupportedOperationException("A ResultSetTuple cannot be written to");
  }
}
