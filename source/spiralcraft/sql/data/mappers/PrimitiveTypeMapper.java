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
package spiralcraft.sql.data.mappers;

import spiralcraft.data.core.PrimitiveTypeImpl;

import spiralcraft.log.ClassLog;
import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.converters.StringifyConverter;
import spiralcraft.sql.model.Column;

import spiralcraft.sql.SqlType;


@SuppressWarnings({ "unchecked", "rawtypes" }) // Requires runtime reflection
public class PrimitiveTypeMapper<T>
    extends TypeMapper<PrimitiveTypeImpl>
{

  protected static final ClassLog log
    =ClassLog.getInstance(PrimitiveTypeMapper.class);
  
  @Override
  public Class<PrimitiveTypeImpl> getTypeClass()
  { return PrimitiveTypeImpl.class;
  }
  
  @Override
  public SqlType getSqlType(PrimitiveTypeImpl type)
  {
    SqlType sqlType=SqlType.getStandardSqlType(type.getNativeClass());
    if (sqlType==null && type.isStringEncodable())
    { sqlType=SqlType.getStandardSqlType(String.class);
    }
      
    if (sqlType==null)
    {
      throw new IllegalArgumentException
        ("Primitive datatype not mapped and not encodable as a String: "
        +type.getNativeClass()
        );
    }
    
    return dialect.getSqlType(sqlType.getTypeId());
  }
  
  @Override
  public void specifyColumn(PrimitiveTypeImpl type, Column col)
  {
    SqlType sqlType=getSqlType(type);
    col.setType(sqlType);
  }
  
    @Override
  public Converter<?,?> getConverter(PrimitiveTypeImpl type)
  { 
    Converter<?,?> ret;
    
    SqlType sqlType=SqlType.getStandardSqlType(type.getNativeClass());
    if (sqlType!=null)
    { 
      sqlType=dialect.getSqlType(sqlType.getTypeId());
      ret=sqlType.getConverter();
    }
    else if (type.isStringEncodable())
    { ret=StringifyConverter.getInstance(type);
    }
    else
    {
      throw new IllegalArgumentException
        ("Primitive datatype not mapped and not encodable as a String: "
        +type.getNativeClass()
        );
    }
    // log.fine("Converter for "+type+" to "+sqlType+" is "+ret);
    return ret;
  }

}
