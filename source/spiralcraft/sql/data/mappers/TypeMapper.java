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

import spiralcraft.data.Type;

import spiralcraft.sql.SqlType;
import spiralcraft.sql.Dialect;

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.model.Column;



/**
 * <P>Maps a Type to a SqlType and a set of column attributes.
 *   
 * <P>The SqlType and the column attributes may depend on 
 *   how the Dialect interprets the Type attributes (ie. length, precision, scale)
 *   according to database specific thresholds. 
 *   
 */
@SuppressWarnings("rawtypes")
public abstract class TypeMapper<T extends Type>
{
  
  private static final TypeMapper[] STANDARD_MAPPERS
    =new TypeMapper[] 
    { new PrimitiveTypeMapper()
    , new BigDecimalTypeMapper()
    , new StringTypeMapper()
    , new TypeTypeMapper()
    };
    
  public static TypeMapper<?>[] getStandardTypeMappers()
  { return STANDARD_MAPPERS;
  }
  
  protected Dialect dialect;
  
  public void setDialect(Dialect dialect)
  { this.dialect=dialect;
  }
  
  public abstract Class<T> getTypeClass();
  
  /**
   * 
   * @return A SQL dialect specific SqlType
   */
  public abstract SqlType<?> getSqlType(T type);
  
  /**
   * Setup the SqlType, length, decimals, and other attributes of the column
   *   from the specific type.
   */
  public abstract void specifyColumn(T type,Column col);
  
  public Converter<?,?> getConverter(T type)
  { return getSqlType(type).getConverter();
  }

}
