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

import java.io.InputStream;
import java.io.IOException;

import java.sql.SQLException;

/**
 * Convert an object to a Short
 */
public class ShortConverter
	implements Converter
{
	public Object convert(Object value)
		throws SQLException
	{
		if (value==null)
		{ return null;
		}
    else if (value instanceof Short)
    { return value;
    }
    else if (value instanceof Number)
    { return new Short(((Number) value).shortValue());
    }
		else if (value instanceof String)
		{ return Short.valueOf((String) value);
		}
		throw new SQLException("Could not safely convert object of type '"+value.getClass().getName()+"' to a Short.");
	}
}
