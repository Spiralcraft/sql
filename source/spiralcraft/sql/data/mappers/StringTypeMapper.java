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

import spiralcraft.data.types.standard.StringType;


import spiralcraft.sql.model.Column;

import spiralcraft.sql.SqlType;

import java.sql.Types;

public class StringTypeMapper
    extends TypeMapper<StringType>
{

  public Class<StringType> getTypeClass()
  { return StringType.class;
  }
  
  public SqlType getSqlType(StringType type)
  {
    
    if (type.getMaxLength()==0 
        || type.getMaxLength()>dialect.getMaximumVarcharSize()
        ) 
    { return dialect.getSqlType(Types.LONGVARCHAR);
    }
    else
    { return dialect.getSqlType(Types.VARCHAR);
    }
  }
  
  @Override
  public void specifyColumn(StringType type, Column col)
  {

    SqlType sqlType=getSqlType(type);
    col.setType(sqlType);
    if (type.getMaxLength()>0)
    { col.setLength(type.getMaxLength());
    }
    
    // TODO Auto-generated method stub

  }

}
