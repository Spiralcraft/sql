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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import spiralcraft.sql.Dialect;
import spiralcraft.sql.ddl.CreateIndexStatement;
import spiralcraft.sql.ddl.DDLStatement;
import spiralcraft.sql.ddl.IndexDefinition;
import spiralcraft.util.ArrayUtil;

public class Index
{
  
  public static Index[] readIndexMetaData(Table table,ResultSet rs)
    throws SQLException
  {
    ArrayList<Index> indices=new ArrayList<>();
    Index index=null;
    String currentIndexName=null;
    List<Column> columns=null;
    while (rs.next())
    {
      if (!rs.getString(6).equals(currentIndexName))
      {
        if (index!=null)
        { 
          index.setColumns(columns.toArray(new Column[columns.size()]));
          indices.add(index);
        }
        currentIndexName=rs.getString(6);
        columns=new ArrayList<Column>();
        index=new Index();
        index.name=currentIndexName;
        index.unique=Boolean.FALSE.equals(rs.getBoolean(4));
        index.table=table;
        
      }
      
      int seq=rs.getInt(8)-1;
      String columnName=rs.getString(9);
      
      while (columns.size()<=seq)
      { columns.add(null);
      }
      columns.set(seq, table.getColumn(columnName));        
      
    }
    if (index!=null)
    { 
      index.setColumns(columns.toArray(new Column[columns.size()]));
      indices.add(index);
    }
    return indices.toArray(new Index[indices.size()]);
  }
  
  private boolean unique;
  private Table table;
  private String name;
  private Column[] columns;
  
  public Index()
  {
  }
  
  public boolean isUnique()
  { return unique;
  }
  
  public Table getTable()
  { return table;
  }
  
  public void setTable(Table table)
  { this.table=table;
  }
  
  public void generateUniqueName()
  { name=table.getName()+"_"+ArrayUtil.format(getFieldNames(), "_", null);
  }
  
  public String getName()
  { return name;
  }
  
  public void setColumns(Column[] columns)
  { this.columns=columns;
  }
  
  public Column[] getColumns()
  { return columns;
  }
  
  public String[] getFieldNames()
  {
    String[] fieldNames=new String[columns.length];
    for (int i=0;i<columns.length;i++)
    { fieldNames[i]=columns[i].getName();
    }
    return fieldNames;
  }
  
  public boolean isFieldEquivalent(Index index)
  { 
    
    if (columns.length==0)
    { return false;
    }
    
    if (index==null)
    { return false;
    }
    
    Column[] otherCols=index.getColumns();
    int i=0;
    for (Column col: columns)
    { 
      if (otherCols.length<=i || !col.getName().equals(otherCols[i].getName()))
      { return false;
      }
      i++;
    }
    return true;
  }  
  
  public List<DDLStatement> generateUpdateDDL(Dialect dialect,Index storeVersion)
  {
    ArrayList<DDLStatement> ret=new ArrayList<DDLStatement>();
    if (storeVersion==null && !isUnique())
    { 
      IndexDefinition indexDef
        =generateIndexDefinition(dialect);
      if (indexDef!=null)
      {
        CreateIndexStatement statement
          =new CreateIndexStatement
            (table.getSchemaName()
            ,table.getName()
            ,name
            ,generateIndexDefinition(dialect)
            );
        ret.add(statement);
      }
    }
    else
    {
      if (!name.equals(storeVersion.getName()))
      {
        
      }
    }
    return ret;
  }    
  
  /**
   * @param dialect
   */
  public IndexDefinition generateIndexDefinition(Dialect dialect)
  { 
    if (getFieldNames().length==0)
    { return null;
    }
    
    return new IndexDefinition(getFieldNames());
  }

  public String toString()
  { return 
      super.toString()+": "+name+" on "
        +ArrayUtil.format(getFieldNames(),",",null);
  }
  
}
