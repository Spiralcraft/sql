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

import spiralcraft.data.types.standard.BigDecimalType;


import spiralcraft.sql.model.Column;

import spiralcraft.sql.SqlType;

import java.sql.Types;

public class BigDecimalTypeMapper
    extends TypeMapper<BigDecimalType>
{

  @Override
  public Class<BigDecimalType> getTypeClass()
  { return BigDecimalType.class;
  }
  
  @Override
  public SqlType<?> getSqlType(BigDecimalType type)
  { return dialect.getSqlType(Types.NUMERIC);
  }
  
  @Override
  public void specifyColumn(BigDecimalType type, Column col)
  {

    SqlType<?> sqlType=getSqlType(type);
    col.setType(sqlType);
    if (type.getPrecision()>0)
    { col.setLength(type.getPrecision());
    }
    if (type.getScale()>0)
    { col.setLength(type.getScale());
    }
    dialect.specifyColumn(type,col);
    
  }

}
