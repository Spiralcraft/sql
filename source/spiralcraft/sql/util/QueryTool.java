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
package spiralcraft.sql.util;


import spiralcraft.vfs.Resolver;
import spiralcraft.vfs.Resource;
import spiralcraft.vfs.StreamUtil;

import spiralcraft.cli.BeanArguments;
import spiralcraft.exec.ExecutionContext;

import java.net.URI;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;

public class QueryTool
  extends AbstractSqlTool
{
  private List<String> sqlList=new ArrayList<String>();
  private boolean update=false;
  
  @Override
  public void execute(String ... args)
  {
    
    new BeanArguments<QueryTool>(this)
    { 
      @Override
      protected boolean processArgument(String argument)
      { 
        addSql(argument);
        return true;
      }
    }
    .process(args);

    run();
  }
  
  public void setUpdate(boolean val)
  { update=val;
  }
  
  public void setScriptURI(URI scriptUri)
    throws IOException
  {
    Resource resource
      =Resolver.getInstance().resolve
        (ExecutionContext.getInstance().canonicalize(scriptUri));
    InputStream in=resource.getInputStream();
    addSql(new String(StreamUtil.readBytes(in)));
    in.close();  
  }
  
  public void addSql(String sql)
  { sqlList.add(sql);
  }
    
  @Override
  public void doWork()
  {
    
    try
    {
      for (String sql : sqlList)
      { executeQuery(sql,update);
      }
    }
    catch (Exception x)
    { error("Caught exception executing sql",x);
    }
  }
    
}