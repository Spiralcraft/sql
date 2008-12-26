//
//Copyright (c) 1998,2007 Michael Toth
//Spiralcraft Inc., All Rights Reserved
//
//This package is part of the Spiralcraft project and is licensed under
//a multiple-license framework.
//
//You may not use this file except in compliance with the terms found in the
//SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
//at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
//Unless otherwise agreed to in writing, this software is distributed on an
//"AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql.data.store;

import spiralcraft.data.lang.CursorBinding;

import spiralcraft.lang.BindException;

import spiralcraft.data.access.Cursor;

import spiralcraft.data.Tuple;

public class SqlBinding<T extends Tuple,C extends Cursor<T>>
  extends CursorBinding<T,C>
{
  public SqlBinding(C cursor)
    throws BindException
  { super(cursor);
  }
  
  /**
   * 
   * @param alias
   * @return
   */
  public TableMapping getTableMapping(String alias)
  {
    return null;
  }

}
