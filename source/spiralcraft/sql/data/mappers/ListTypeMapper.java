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

import spiralcraft.data.core.AbstractCollectionType;


import spiralcraft.sql.model.Column;

import spiralcraft.sql.SqlType;

import java.sql.Types;

public class ListTypeMapper
    extends TypeMapper<AbstractCollectionType<?,?>>
{

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Class<AbstractCollectionType<?,?>> getTypeClass()
  { return (Class<AbstractCollectionType<?,?>>) (Class) AbstractCollectionType.class;
  }
  
  @Override
  public SqlType<?> getSqlType(AbstractCollectionType<?,?> type)
  { return dialect.getSqlType(Types.ARRAY);
  }
  
  @Override
  public void specifyColumn(AbstractCollectionType<?,?> type, Column col)
  {

    SqlType<?> sqlType=getSqlType(type);
    col.setType(sqlType);

    
  }

}
