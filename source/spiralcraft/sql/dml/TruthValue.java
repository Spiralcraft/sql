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

package spiralcraft.sql.dml;

import java.util.List;

import spiralcraft.sql.SqlFragment;

public class TruthValue
  extends SqlFragment
{
  private Boolean value;
  
  public TruthValue()
  { }
  
  public TruthValue(boolean val)
  {
    if (val)
    { value=Boolean.TRUE;
    }
    else
    { value=Boolean.FALSE;
    }
  }
  
  public void write(StringBuilder buffer,String indent, List parameterCollector)
  {
    if (value==null)
    { buffer.append(" NULL ");
    }
    else if (value.booleanValue())
    { buffer.append(" TRUE ");
    }
    else 
    { buffer.append(" FALSE ");
    }
   
  }
}
