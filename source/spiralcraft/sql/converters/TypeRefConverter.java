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
import spiralcraft.data.Tuple;
import spiralcraft.data.TypeResolver;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;

/**
 * Convert an intermediate type reference Tuple to a URI type reference
 */
public class TypeRefConverter
	extends Converter<String,Tuple>
{
  private static final TypeRefConverter instance=new TypeRefConverter();
  
  public static final TypeRefConverter getInstance()
  { return instance;
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public TypeRefConverter()
  { super(Tuple.class);
  }
  
	@Override
  public String toSql(Tuple value)
		throws SQLException
	{ return value.getType().getURI().toString();
	}
  
  @Override
  public Tuple fromSql(String value)
    throws SQLException
  { 
    try
    { 
      return TypeResolver.getTypeResolver().resolve(new URI(value))
        .getReference();
    }
    catch (URISyntaxException x)
    { throw new SQLException("Invalid URI syntax: "+value,x);
    }
    catch (DataException x)
    { 
      throw new SQLException("Error resolving type "+value,x);
    }
  }
  
  @Override
  public Class<Tuple> getSqlClass()
  { return Tuple.class;
  }
}
