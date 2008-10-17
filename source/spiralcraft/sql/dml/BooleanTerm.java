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

/**
 * A SQL "Boolean Term" or "AND" expression
 */
public class BooleanTerm
  extends BooleanCondition
{
  private BooleanCondition booleanTerm;
  private BooleanCondition booleanFactor;
  
  public BooleanTerm(BooleanCondition booleanTerm,BooleanCondition booleanFactor)
  { 
    this.booleanTerm=booleanTerm;
    this.booleanFactor=booleanFactor;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    booleanTerm.write(buffer,indent, parameterCollector);
    buffer.append("\r\n").append(indent).append(" AND ");
    indent=indent+"  ";
    booleanFactor.write(buffer,indent, parameterCollector);
  }
  
  @Override
  public int getPrecedence()
  { return 2;
  }
}
