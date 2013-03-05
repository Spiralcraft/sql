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

import spiralcraft.data.DataException;
import spiralcraft.data.Type;


import java.sql.SQLException;

/**
 * Convert an object to a sql Varchar String
 */
public class StringifyConverter<T>
	extends Converter<String,T>
{
  public static final <T> StringifyConverter<T> getInstance(Type<T> t)
  { return new StringifyConverter<T>(t);
  }
  
  
  private final Type<T> type;

  protected StringifyConverter(Type<T> type)
  { 
    super(type.getNativeClass());
    this.type=type;
  }
  
	@Override
  public String toSql(T value)
		throws SQLException
	{ return type.toString(value);
	}
  
  @Override
  public T fromSql(String value)
    throws SQLException
  { 
    try
    { return type.fromString(value);
    }
    catch (DataException x)
    { 
      throw new SQLException
        ("Error translating string to object for type "+type.getURI()
        +" ["+value+"]"
        ,x
        );
    }
  }
  
  @Override
  public Class<String> getSqlClass()
  { return String.class;
  }
}
