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
package spiralcraft.sql.model;

import java.util.ArrayList;
import java.util.List;

import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;
import spiralcraft.sql.Dialect;
import spiralcraft.sql.ddl.AddTableConstraintDefinition;
import spiralcraft.sql.ddl.AlterTableStatement;
import spiralcraft.sql.ddl.DDLStatement;
import spiralcraft.sql.ddl.DropPrimaryKeyConstraintDefinition;
import spiralcraft.sql.ddl.DropTableConstraintDefinition;
import spiralcraft.sql.ddl.PrimaryKeyConstraint;
import spiralcraft.sql.ddl.UniqueConstraint;
import spiralcraft.sql.ddl.TableConstraintDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;

public class KeyConstraint
{
  
  private static final ClassLog log=ClassLog.getInstance(KeyConstraint.class);  
  private static final Level logLevel
    =ClassLog.getInitialDebugLevel(KeyConstraint.class,Level.INFO);

  private Column[] columns;
  private boolean primary;
  private boolean unique;
  private Table table;
  private String constraintName;
  
  public KeyConstraint()
  {
  }

  public KeyConstraint(String constraintName,Table table,ResultSet rs,boolean primary)
    throws SQLException
  {
    this.constraintName=constraintName;
    this.table=table;
    List<Column> columns=new ArrayList<Column>();
    boolean hasConstraintName=rs.getMetaData().getColumnCount()>5;
    while (rs.next())
    {
      String columnName=rs.getString(4);
      int seq=rs.getInt(5)-1;
      
      while (columns.size()<=seq)
      { columns.add(null);
      }
      columns.set(seq, table.getColumn(columnName));
      if (logLevel.isDebug())
      { log.fine("KeyConstraint: "+columnName+" "+columns.get(seq));
      }
      
      if (hasConstraintName)
      {
        String rsConstraintName=rs.getString(6);
        if (constraintName==null && rsConstraintName!=null)
        { 
          constraintName=rsConstraintName;
          if (logLevel.isDebug())
          { log.fine("Constraint name is "+constraintName);
          }
        }
        
      }
      
    }
    this.columns=columns.toArray(new Column[columns.size()]);
    this.primary=primary;
  }
  
  public String getConstraintName()
  { return constraintName;
  }
  
  public void setTable(Table table)
  { this.table=table;
  }
  
  public void setPrimary(boolean primary)
  { this.primary=primary;
  }
  
  public boolean isPrimary()
  { return primary;
  }
  
  public void setUnique(boolean unique)
  { this.unique=unique;
  }
  
  public boolean isUnique()
  { return unique;
  }

  public void setColumns(Column[] columns)
  { this.columns=columns;
  }
  
  public Column[] getColumns()
  { return columns;
  }
  
  public boolean isFieldEquivalent(KeyConstraint keyConstraint)
  { 
    
    if (columns.length==0)
    { return false;
    }
    
    if (keyConstraint==null)
    { return false;
    }
    
    Column[] otherCols=keyConstraint.getColumns();
    int i=0;
    for (Column col: columns)
    { 
      if (!col.getName().equals(otherCols[i].getName()))
      { return false;
      }
      i++;
    }
    return true;
  }
  
  public String[] getFieldNames()
  {
    String[] fieldNames=new String[columns.length];
    for (int i=0;i<columns.length;i++)
    { fieldNames[i]=columns[i].getName();
    }
    return fieldNames;
  }
  
  /**
   * @param dialect
   */
  public TableConstraintDefinition generateConstraintDefinition(Dialect dialect)
  { 
    if (getFieldNames().length==0)
    { return null;
    }
    if (primary)
    { return new TableConstraintDefinition(new PrimaryKeyConstraint(getFieldNames()));
    }
    else if (unique)
    { return new TableConstraintDefinition(new UniqueConstraint(getFieldNames()));
    }
    
    return null;
  }
  
  public DDLStatement generateDropDDL(Dialect dialect)
  { 
    if (constraintName!=null)
    {
      return table.createAlterTableStatement
          (new DropTableConstraintDefinition(constraintName)
          );
    }
    else if (primary)
    {
      return table.createAlterTableStatement
          (new DropPrimaryKeyConstraintDefinition());
    }
    else
    { 
      log.warning("Unable to drop unnamed constraint "+this);
      return DDLStatement.EMPTY;
    }
  }
  
  public List<DDLStatement> generateUpdateDDL(Dialect dialect,KeyConstraint storeVersion)
  {
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    if (storeVersion==null)
    { 
      TableConstraintDefinition constraint
        =generateConstraintDefinition(dialect);
      if (constraint!=null)
      {
        AlterTableStatement statement
          =table.createAlterTableStatement
            (new AddTableConstraintDefinition
              (constraint
              )
            );
        ret.add(statement);
      }
    }
    else
    {
    }
    return ret;
  }  
}
