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

import spiralcraft.data.TypeResolver;
import spiralcraft.data.DataException;
import spiralcraft.data.EditableTuple;
import spiralcraft.data.Field;

import spiralcraft.data.core.FieldImpl;
import spiralcraft.data.core.SchemeImpl;

import spiralcraft.sql.SqlType;

import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * A Scheme created from ResultSetMetaData
 */
public class ResultSetScheme
  extends SchemeImpl
{
  
  @SuppressWarnings("unchecked") // Inherently unsafe ops
  public ResultSetScheme(ResultSetMetaData metadata)
    throws DataException
  {
    try
    {
      int count=metadata.getColumnCount();
      for (int i=1;i<count+1;i++)
      { 
        FieldImpl field=new FieldImpl<Object>();
        String mdName=metadata.getColumnName(i);
        
        // TODO: Screen for illegal characters
        String goodName=mdName;
        
        int suffix=0;
        while (localFieldMap.get(goodName)!=null)
        { goodName=mdName+Integer.toString(++suffix);
        } 
        field.setName(goodName);

        Class<?> typeClass=SqlType.getSqlType(metadata.getColumnType(i)).getSqlClass();
        if (typeClass==null)
        { 
          System.err.println
            ("ResultSetScheme: No mapped type for "
                +SqlType.getSqlType(metadata.getColumnType(i)).getName()
            );
          typeClass=Object.class;
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
  

  @SuppressWarnings("unchecked") // Inherently unsafe ops
  public void readResultSet(ResultSet rs,EditableTuple tuple)
    throws SQLException,DataException
  {
    for (Field field : fields)
    { field.setValue(tuple,rs.getObject(field.getIndex()+1));
    }
  }
}
