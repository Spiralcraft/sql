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
import spiralcraft.data.spi.ArrayTuple;

import spiralcraft.data.DeltaTuple;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Scheme;
import spiralcraft.data.Tuple;
import spiralcraft.data.DataException;


import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A Tuple that retrieves data from a ResultSet. If the ResultSet is advanced,
 *   the Tuple's data will change.
 */
public class ResultSetTuple
  extends AbstractTuple
  implements Tuple
{
  private ResultSet resultSet;
  private ResultSetMapping mapping;
  private ResultSetTuple[] subs;
  private String statementInfo;
  
  public ResultSetTuple(FieldSet fieldSet)
  { 
    super(fieldSet instanceof Scheme?fieldSet:fieldSet.getType().getScheme());    
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
  
  public ResultSetTuple(FieldSet fieldSet,ResultSetMapping mapping)
  {
    this(fieldSet instanceof Scheme?fieldSet:fieldSet.getType().getScheme());
    applyMapping(mapping);
  }
  
  public void setStatementInfo(String statementInfo)
  {
    this.statementInfo=statementInfo;
    if (baseExtent!=null)
    { ((ResultSetTuple) baseExtent).setStatementInfo(statementInfo);
    }
  }
  
  private void applyMapping(ResultSetMapping mapping)
  { 
    this.mapping=mapping;
    
    if (baseExtent!=null)
    { ((ResultSetTuple) baseExtent).applyMapping(mapping.baseExtent);
    }
    
    if (mapping.subs!=null)
    { 
      subs=new ResultSetTuple[mapping.subs.length];
      int i=0;
      for (ResultSetMapping subMapping: mapping.subs)
      {
        if (subMapping!=null)
        { subs[i]=new ResultSetTuple(subMapping.fieldSet,subMapping);
        }
        i++;
      }
    }
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
    
    ResultColumnMapping col=mapping.map[index];
    if (col!=null && col.resultSetColumn>0)
    {
      try
      { 
        Object sqlValue=resultSet.getObject(col.resultSetColumn);
        if (col.converter!=null)
        { 
          try
          { return col.converter.fromSql(sqlValue);
          }
          catch (ClassCastException x)
          {
            throw new DataException
              ("Unexpected data type "
              +sqlValue.getClass().getName()
              +" ["+sqlValue+"] applied to "+col.converter
              +" for column "+col.resultSetColumn+" ("
              +resultSet.getMetaData().getColumnName(col.resultSetColumn)+")"
              ,x
              );
          }
        }
        else
        { return sqlValue;
        }
      }
      catch (SQLException x)
      { 
        throw new DataException
          ("Error reading result set column for field index "+index
            +" ("+fieldSet.getFieldByIndex(index).getURI()+") from: "
            +statementInfo+": mapping="
            +col+": rs="+resultSet
          ,x);
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

  @Override
  protected AbstractTuple copyTupleField(
    Tuple fieldValue)
    throws DataException
  { return new ArrayTuple(fieldValue);
  }
}
