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

import spiralcraft.log.ClassLog;
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
  private static final ClassLog log=ClassLog.getInstance(ResultSetScheme.class);
  
  @SuppressWarnings({ "unchecked", "rawtypes" }) // Inherently unsafe ops
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
        
        mdName=sanitize(mdName);
        String goodName=mdName;
        
        int suffix=0;
        while (localFieldMap.get(goodName)!=null)
        { goodName=mdName+Integer.toString(++suffix);
        } 
        field.setName(goodName);

        Class<?> typeClass=SqlType.getSqlType(metadata.getColumnType(i)).getSqlClass();
        if (typeClass==null)
        { 
          log.fine
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
  

  private String sanitize(String name)
  {
    StringBuilder newName=new StringBuilder();
    for (int i=0;i<name.length();i++)
    { 
      char chr=name.charAt(i);
      if (i==0 && Character.isDigit(chr))
      { newName.append("_");
      }
      if (Character.isJavaIdentifierPart(chr))
      { newName.append(chr);
      }
      else
      { newName.append("_x"+Integer.toHexString(chr)+"_");
      }
    }
    return newName.toString();
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" }) // Inherently unsafe ops
  public void readResultSet(ResultSet rs,EditableTuple tuple)
    throws SQLException,DataException
  {
    for (Field field : fields)
    { field.setValue(tuple,rs.getObject(field.getIndex()+1));
    }
  }
}
