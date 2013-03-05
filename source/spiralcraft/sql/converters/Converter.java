//
// Copyright (c) 1998,2005 Michael Toth
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
package spiralcraft.sql.converters;

import java.lang.reflect.Array;
import java.sql.SQLException;

import spiralcraft.data.Aggregate;
import spiralcraft.data.DataException;
import spiralcraft.util.ArrayUtil;

/**
 * Represents a type conversion to a SQL "friendly" type from another 
 *   type.
 */
public abstract class Converter<Ts,Tj>
{
  private Converter<Object,Tj[]> arrayConverter;
  private final Class<? extends Tj> nativeClass;
  
	public abstract Ts toSql(Tj nativeValue)
		throws SQLException;
  
  public abstract Tj fromSql(Ts sqlValue)
    throws SQLException;
  
  public abstract Class<?> getSqlClass();
  
  protected Converter(Class<? extends Tj> nativeClass)
  { this.nativeClass=nativeClass;
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public synchronized Converter<Object,Tj[]> arrayConverter()
  { 
    if (arrayConverter==null)
    { 
      arrayConverter=(Converter) 
       new ArrayConverter
         ((Class<Ts>) getSqlClass()
         ,nativeClass!=null
           ?nativeClass
           :Object.class
         );
    }
    return arrayConverter;
  }
  
  class ArrayConverter
    extends Converter<Object,Object>
  {
    
    private Class<Ts> sqlClass;
    
    ArrayConverter(Class<Ts> sqlClass,Class<? extends Object> nativeClass)
    { 
      super
        (ArrayUtil.arrayClass
          (nativeClass
          )
        );
      this.sqlClass=sqlClass;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Object toSql(
      Object nativeValue)
      throws SQLException
    {
      if (nativeValue==null)
      { return null;
      }
      try
      {
        Tj[] array;
        if (nativeValue instanceof Aggregate)
        { array= (Tj[]) ((Aggregate<Tj>) nativeValue).getType()
            .fromData((Aggregate<Tj>) nativeValue,null);
        }
        else
        { array=(Tj[]) nativeValue;
        }
        
        Ts[] sqlArray=(Ts[]) Array.newInstance(sqlClass,array.length);
        int i=0;
        for (Tj jobject : array)
        { sqlArray[i++]=Converter.this.toSql(jobject);
        }
        return sqlArray;
      }
      catch (DataException x)
      { throw new SQLException(x);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object fromSql(
      Object sqlValue)
      throws SQLException
    {
      if (sqlValue==null)
      { return null;
      }
      
      Ts[] sqlArray;
      if (sqlValue instanceof java.sql.Array)
      { 
        sqlArray=(Ts[]) ((java.sql.Array) sqlValue).getArray();
      }
      else if (sqlValue.getClass().isArray())
      { sqlArray=(Ts[]) sqlValue;
      }
      else
      { throw new SQLException("Unknown object "+sqlValue);
      }
      
      Object[] array=(Object[]) Array.newInstance(Converter.this.nativeClass,sqlArray.length);
      int i=0;
      for (Ts sqlObject: sqlArray)
      { array[i++]=Converter.this.fromSql(sqlObject);
      }
      return array;
    }

    @Override
    public Class<?> getSqlClass()
    { return Array.newInstance(Converter.this.getSqlClass(),0).getClass();
    } 
    
  }
  
}
