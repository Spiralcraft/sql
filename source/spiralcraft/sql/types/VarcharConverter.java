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
package spiralcraft.sql.types;

import spiralcraft.stream.StreamUtil;

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a sql Varchar String
 */
public class VarcharConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
		else if (value instanceof String)
		{ return value;
		}
    else if (value instanceof Character)
    { return value.toString();
    }
		else if (value instanceof InputStream)
		{
			try
			{ return new String(StreamUtil.readBytes((InputStream) value));
			}
			catch (IOException x)
			{ throw new SQLException("Error reading InputStream to a String during convertion to JDBC VARCHAR type: "+x.getMessage());
			}
		}
		else if (value instanceof Number)
		{ return value.toString();
		}
		else if (value instanceof byte[])
		{ return new String((byte[]) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a String.");
	}
  
  public Class getTargetClass()
  { return String.class;
  }
}
