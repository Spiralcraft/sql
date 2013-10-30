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

import spiralcraft.data.types.meta.TypeType;

import spiralcraft.log.ClassLog;
import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.converters.TypeRefConverter;
import spiralcraft.sql.model.Column;

import spiralcraft.sql.SqlType;


@SuppressWarnings({ "unchecked", "rawtypes" }) // Requires runtime reflection
public class TypeTypeMapper<T>
    extends TypeMapper<TypeType>
{

  protected static final ClassLog log
    =ClassLog.getInstance(TypeTypeMapper.class);
  
  @Override
  public Class<TypeType> getTypeClass()
  { return TypeType.class;
  }
  
  @Override
  public SqlType getSqlType(TypeType type)
  { return dialect.getSqlType(SqlType.getStandardSqlType(String.class).getTypeId());
  }
  
  @Override
  public void specifyColumn(TypeType type, Column col)
  {
    SqlType sqlType=getSqlType(type);
    col.setType(sqlType);
    dialect.specifyColumn(type,col);
  }
  
    @Override
  public Converter<?,?> getConverter(TypeType type)
  { return TypeRefConverter.getInstance();
  }

}
