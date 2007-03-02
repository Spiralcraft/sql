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
package spiralcraft.sql.meta;

import spiralcraft.data.TypeResolver;
import spiralcraft.data.DataException;
import spiralcraft.data.EditableTuple;
import spiralcraft.data.Field;

import spiralcraft.data.core.FieldImpl;
import spiralcraft.data.core.SchemeImpl;

import spiralcraft.sql.types.TypeMap;

import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * A Scheme created from ResultSetMetaData
 */
public class ResultSetScheme
  extends SchemeImpl
{
  
  public ResultSetScheme(ResultSetMetaData metadata)
    throws DataException
  {
    try
    {
      int count=metadata.getColumnCount();
      for (int i=1;i<count+1;i++)
      { 
        FieldImpl field=new FieldImpl();
        field.setName(metadata.getColumnName(i));

        Class typeClass=TypeMap.getJavaClassFromSqlType(metadata.getColumnType(i));
        if (typeClass==null)
        { typeClass=Object.class;
        }

        field.setType
        (TypeResolver.getTypeResolver()
            .resolveFromClass(typeClass)
        );
        addField(field);
      }
    }
    catch (SQLException x)
    { throw new DataException(x.toString(),x);
    }
  }
  
  public void readResultSet(ResultSet rs,EditableTuple tuple)
    throws SQLException,DataException
  {
    for (Field field : fields)
    { field.setValue(tuple,rs.getObject(field.getIndex()+1));
    }
  }
}
