//
// Copyright (c) 1998,2005 Michael Toth
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


import spiralcraft.data.Field;
import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;
import spiralcraft.data.FieldSet;

import spiralcraft.data.DataConsumer; 

import spiralcraft.data.flatfile.Parser;
import spiralcraft.data.flatfile.ParseException;

import spiralcraft.data.core.KeyImpl;

import spiralcraft.data.lang.TupleFocus;
import spiralcraft.data.persist.PersistenceException;

import spiralcraft.vfs.Resource;
import spiralcraft.vfs.Resolver;
import spiralcraft.vfs.UnresolvableURIException;



import spiralcraft.lang.BindException;
import spiralcraft.lang.Channel;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;
import spiralcraft.sql.Constants;

import spiralcraft.sql.SqlType;

import spiralcraft.exec.Executable;
import spiralcraft.exec.ExecutionContext;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileWriter;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.ResultSet;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.HashMap;

import java.util.zip.GZIPInputStream;

import java.net.URI;

import java.math.BigDecimal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


/**
 * Loads a SQL database from an input file
 */
public class Loader
  implements Executable
{

  private static final ClassLog log=ClassLog.getInstance(Loader.class);
  private Resource _schemaResource;
  private CommonDataSource dataSource;
  private String _tableName;
  private Connection _connection;
  private XAConnection _xaConnection;
  private PreparedStatement _statement;
  private int _count;
  private int _transactionSize=1000;
  private int _statusInterval=100;
  private int _skipCount=0;
  private String _updateKeyFields=null;
  private String _insertKeyFields=null;
  private int[] _paramMap=null;
  private int[] _typeMap=null;
  private boolean _checkKey=false;
  private File _discardFile=null;
  private PrintWriter _discardWriter;
  private boolean _implicitTypes;
  private char _delimiter=',';
  private boolean _delete=false;
  private boolean _gzip=false;
  private ArrayList<Resource> _inputResources=new ArrayList<Resource>();
  private boolean _truncate=false;
  private boolean _autoCommit=false;
  private String _connectionSetupSql;
  private Parser parser;
  private HashMap<Tuple,Tuple> _keyMap=new HashMap<Tuple,Tuple>();
  private DateFormat _dateFormat
    =new SimpleDateFormat(Constants.JDBC_TIMESTAMP_ESCAPE_FORMAT);
  { _dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  
  @Override
  public void execute(String ... args)
  {
    ExecutionContext context
      =ExecutionContext.getInstance();
//    if (args.length==0)
//    {
//      context.err().println(usage());
//      return;
//    }
    try
    {
  
      for (int i=0;i<args.length;i++)
      {
        if (args[i].startsWith("-"))
        { 
          String option=args[i].substring(1).toLowerCase();
          if (option.equals("table"))
          { setTableName(args[++i]);
          }
          else if (option.equals("database"))
          { 
            try
            {
              setDataSource
                (new ResourceDataSource
                    (context.canonicalize
                      (URI.create(args[++i]))
                    )
                );
            }
            catch (PersistenceException x)
            { throw new IllegalArgumentException(args[i]+" : "+x,x);
            }
          }
          else if (option.equals("input"))
          { 
            try
            {
              addInputResource
                (Resolver.getInstance().resolve
                  (context.canonicalize
                    (URI.create(args[++i])))
                );
            }
            catch (UnresolvableURIException x)
            { throw new RuntimeException("Error resolving "+args[i],x);
            }
          }
          else if (option.equals("schema"))
          { 
            try
            {
              setSchemaResource
                (Resolver.getInstance().resolve
                  (context.canonicalize
                    (URI.create(args[++i]))
                  )
                );
            }
            catch (UnresolvableURIException x)
            { throw new RuntimeException("Error resolving "+args[i],x);
            }
          }
          else if (option.equals("skip"))
          { setSkipCount(Integer.parseInt(args[++i]));
          }
          else if (option.equals("gzip"))
          { setGzip(true);
          }
          else if (option.equals("updatekey"))
          { setUpdateKeyFields(args[++i]);
          }
          else if (option.equals("insertkey"))
          { setInsertKeyFields(args[++i]);
          }
          else if (option.equals("checkkey"))
          { setCheckKey(true);
          }
          else if (option.equals("discardfile"))
          { setDiscardFile(new File(args[++i]));
          }
          else if (option.equals("useImplicitTypes"))
          { setUseImplicitTypes(true);
          }
          else if (option.equals("delimiter"))
          { setDelimiter(args[++i].charAt(0));
          }
          else if (option.equals("delete"))
          { setDelete(true);
          }
          else if (option.equals("truncate"))
          { setTruncate(true);
          }
          else if (option.equals("nobatch"))
          { setAutoCommit(true);
          }
          else if (option.equals("connectionsetupsql"))
          { setConnectionSetupSql(args[++i]);
          }
          else if (option.equals("transactionsize"))
          { setTransactionSize(Integer.parseInt(args[++i]));
          }
          else if (option.equals("statusinterval"))
          { setStatusInterval(Integer.parseInt(args[++i]));
          }
          else
          { 
            log.severe("Unknown option '-"+option+"'");
//            System.err.println(usage());
            return;
          }
        }
        else
        { addInputResource(Resolver.getInstance().resolve(URI.create(args[i])));
        }
      }
      run();
    }
    catch (IOException x)
    { x.printStackTrace();
    }
  }


  public void setParser(Parser parser)
  { this.parser=parser;
  }
  
  
  public void setTransactionSize(int val)
  { _transactionSize=val;
  }
  
  public void setStatusInterval(int val)
  { _statusInterval=val;
  }

  public void setTruncate(boolean val)
  { _truncate=val;
  }
  
  public void setGzip(boolean val)
  { _gzip=val;
  }

  public void setDelete(boolean delete)
  { _delete=delete;
  }

  public void setAutoCommit(boolean val)
  { _autoCommit=val;
  }
  
  public void setConnectionSetupSql(String sql)
  { _connectionSetupSql=sql;
  }
  
 

  public void run()
  {
    if (_tableName==null)
    { throw new RuntimeException("No table name specified");
    }
    if (dataSource==null)
    { throw new RuntimeException("No database specified");
    }
    if (_inputResources.size()==0)
    { throw new RuntimeException("No input files specified");
    }
    
    
    try
    { 
      if (_discardFile!=null)
      { 
        if (!_discardFile.exists())
        { _discardWriter=new PrintWriter(new FileWriter(_discardFile));
        }
        else 
        { 
          throw new IOException
            ("Discard file '"
            +_discardFile.getName()
            +"' already exists."
            );
        }

      }

      if (_inputResources.size()>0)
      {
        Iterator<Resource> it=_inputResources.iterator();
        while (it.hasNext())
        { load(it.next());
        }
      }
      
    }
    catch (IOException x)
    { x.printStackTrace();
    }
    catch (ParseException x)
    { x.printStackTrace();
    }
    catch (SQLException x)
    { x.printStackTrace();
    }
    catch (DataException x)
    { x.printStackTrace();
    }
    finally
    {
      if (_discardWriter!=null)
      {
        _discardWriter.flush();
        _discardWriter.close();
        _discardWriter=null;
      }
    }
    
  }

  private void load(Resource resource)
    throws IOException,ParseException,SQLException,DataException
  {
    
    if (parser==null)
    { 
      log.info("Creating parser");
      parser=new Parser();
      parser.setReadHeader(true);
      parser.setUseImplicitTypes(_implicitTypes);
      parser.setDelimiter(_delimiter);
    }

    log.info("Checking data in "+resource.getURI()+"...");

    // Prescan data for syntax
    VerifyDataHandler verifyHandler=new VerifyDataHandler();
    if (_schemaResource!=null)
    { 
      parser.readHeaderFromStream
        (_schemaResource.getInputStream());
    }

    if (_gzip)
    { 
      parser.parse
        (new BufferedInputStream
          (new GZIPInputStream
            (resource.getInputStream()
            )
          )
        ,verifyHandler
        )
        ;
    }
    else
    { 
      parser.parse
        (new BufferedInputStream
          (resource.getInputStream()
          )
        ,verifyHandler
        )
        ;
    }

    log.info(verifyHandler.getCount()+" rows checked.");

    
    // Load data
    DataConsumer<Tuple> handler=new SqlDataHandler();

    if (_gzip)
    { 
      parser.parse
        (new BufferedInputStream
          (new GZIPInputStream
            (resource.getInputStream()
            )
          )
        ,handler
        )
        ;
    }
    else
    { 
      parser.parse
        (new BufferedInputStream
          (resource.getInputStream()
          )
          ,handler
        )
        ;
    }

    _connection.commit();
    if (!_checkKey)
    { log.info("Updated "+_count);
    }
    else
    { log.info("Queried "+_count);
    }
    _connection.close();
    if (_xaConnection!=null)
    { _xaConnection.close();
    }
    
  }
  
  
  public void setDataSource(CommonDataSource dataSource)
  { this.dataSource=dataSource;
  }

  public void setTableName(String tableName)
  { _tableName=tableName;
  }

  public void setInputResource(Resource inputResource)
  { 
    _inputResources.clear();
    _inputResources.add(inputResource);
  }

  public void addInputResource(Resource inputResource)
  { _inputResources.add(inputResource);
  }

  public void setSchemaResource(Resource schemaResource)
  { _schemaResource=schemaResource;
  }

  public void setSkipCount(int skipCount)
  { _skipCount=skipCount;
  }
  
  public void setUpdateKeyFields(String updateKeyFields)
  { _updateKeyFields=updateKeyFields;
  }

  public void setInsertKeyFields(String insertKeyFields)
  { _insertKeyFields=insertKeyFields;
  }

  public void setCheckKey(boolean checkKey)
  { _checkKey=checkKey;
  }

  public void setDiscardFile(File discardFile)
  { _discardFile=discardFile;
  }

  public void setUseImplicitTypes(boolean implicitTypes)
  { _implicitTypes=implicitTypes;
  }

  public void setDelimiter(char delimiter)
  { _delimiter=delimiter;
  }

  class VerifyDataHandler
    implements DataConsumer<Tuple>
  {
    private int count=0;
    private TupleFocus<Tuple> dataFocus;
    private FieldSet fieldSet;
    private boolean debug;
    
    @Override
    public void setDebug(boolean debug)
    { this.debug=debug;
    }

    @Override
    public void dataInitialize(FieldSet fieldSet)
      throws DataException
    {
      if (debug)
      { log.info("Fields: "+fieldSet.toString());
      }
      this.fieldSet=fieldSet;

      
      dataFocus
        =TupleFocus.create(null,fieldSet);

      try
      {
        // Check keys
        if (_insertKeyFields!=null)
        { 
          new KeyImpl<Tuple>(fieldSet,_insertKeyFields)
            .bindChannel(dataFocus.getSubject(),dataFocus,null);
        }
      
        if (_updateKeyFields!=null)
        { 
          new KeyImpl<Tuple>(fieldSet,_updateKeyFields)
            .bindChannel(dataFocus.getSubject(),dataFocus,null);
        }
      }
      catch (BindException x)
      { throw new DataException("Error binding SqlDataHandler");
      }
      
        
    }

    public FieldSet dataGetFieldSet()
    { return this.fieldSet;
    }
    
    public Tuple dataGetTuple()
    { return dataFocus.getSubject().get();
    }
    
    @Override
    public void dataAvailable(Tuple data)
      throws DataException
    { 
      count++;
      dataFocus.setTuple(data);
    }
    
    @Override
    public void dataFinalize()
    { 
    }
    
    public int getCount()
    { return count;
    }

  }


	class SqlDataHandler
		implements DataConsumer<Tuple>
	{
    private TupleFocus<Tuple> dataFocus;
    private FieldSet fieldSet;
    private KeyImpl<Tuple> insertKey;
    private KeyImpl<Tuple> updateKey;
    private Channel<Tuple> insertKeyBinding;
    private Channel<Tuple> updateKeyBinding;
    private boolean debug;
    
    @Override
    public void setDebug(boolean debug)
    { this.debug=debug;
    }
    
    @Override
    public void dataInitialize(FieldSet fieldSet)
      throws DataException
    {
      if (debug)
      { log.debug("Initializing updater for "+fieldSet);
      }
      
      this.fieldSet=fieldSet;
      dataFocus
        =TupleFocus.<Tuple>create(null,fieldSet);
      
      try
      {
        // Check keys
        if (_insertKeyFields!=null)
        { 
          insertKey=new KeyImpl<Tuple>(fieldSet,_insertKeyFields);
          insertKeyBinding=insertKey.bindChannel
            (dataFocus.getSubject(),dataFocus,null);
        }
      
        if (_updateKeyFields!=null)
        { 
          updateKey=new KeyImpl<Tuple>(fieldSet,_updateKeyFields);
          updateKeyBinding=updateKey.bindChannel
            (dataFocus.getSubject(),dataFocus,null);
        }
      }
      catch (BindException x)
      { throw new DataException("Error binding SqlDataHandler");
      }
      
      initializeSql();
      
      
      
    }


    public FieldSet dataGetFieldSet()
    { return this.fieldSet;
    }
    
    public Tuple dataGetTuple()
    { return dataFocus.getSubject().get();
    }
    
    @Override
    public void dataAvailable(Tuple data)
      throws DataException
    { 
      dataFocus.setTuple(data);
      performSql();
    }
    
    @Override
    public void dataFinalize()
    { 
    }

    private int translateType(Field<?> field)
    {
      if (field.getType().getNativeClass()!=null)
      { return SqlType.getStandardSqlType(field.getType().getNativeClass()).getTypeId();
      }
      else
      { return Types.VARCHAR;
      }
    }	
			
		public void initializeSql()
		{ 
      try
      {
        if (dataSource instanceof XADataSource)
        {
          _xaConnection=((XADataSource) dataSource).getXAConnection();
          _connection=_xaConnection.getConnection();
        }
        else
        {
          _connection=((DataSource) dataSource).getConnection();
        }
        
        if (_connectionSetupSql!=null)
        { _connection.createStatement().execute(_connectionSetupSql);
        }
        
        if (_truncate)
        { _connection.createStatement().executeUpdate("TRUNCATE TABLE "+quote(_tableName));
        }
            
        _connection.setAutoCommit(_autoCommit);

        int numFields=fieldSet.getFieldCount();
        
        _paramMap=new int[numFields];
        _typeMap=new int[numFields];
        

        if (updateKeyBinding!=null && _checkKey)
        {
          //
          // Prepare a SELECT
          //
          StringBuffer sql=new StringBuffer();
          sql.append("SELECT count(*) FROM "+quote(_tableName)+" WHERE ");

          int count=0;
          int keyCount=0;
          for (int i=0;i<numFields;i++)
          { 
            Field<?> field=fieldSet.getFieldByIndex(i);
            _typeMap[count]=translateType(field);
            String fieldName=field.getName();
            
            if (updateKey.getFieldByName(fieldName)!=null)
            {
              // Note the index of the key fields
              _paramMap[count]=(keyCount+1);
              if (keyCount>0)
              { sql.append(" AND ");
              }

              sql.append(quote(fieldName));
              sql.append("= ?");
              keyCount++;
            }
            else
            { _paramMap[count]=-1;
            }
            count++;
          }

          _statement=_connection.prepareStatement(sql.toString());
          
        }
        else if (updateKeyBinding!=null && !_checkKey)
        {

          //
          // Prepare an UPDATE
          //
          StringBuffer sql1=new StringBuffer();
          StringBuffer sql2=new StringBuffer();
          sql1.append("UPDATE "+quote(_tableName)+" SET ");
          sql2.append(" WHERE ");
          int count=0;
          int fieldCount=0;
          int keyCount=0;
          for (int i=0;i<numFields;i++)
          { 
            Field<?> field=fieldSet.getFieldByIndex(i);
            _typeMap[count]=translateType(field);
            String fieldName=field.getName();
            
            if (updateKey.getFieldByName(fieldName)!=null)
            {
              int updateFieldCount=updateKey.getFieldCount();
              
              // Note the index of the key fields
              _paramMap[count]=( (_paramMap.length-updateFieldCount)+keyCount+1);
              if (keyCount>0)
              { sql2.append(" AND ");
              }
              sql2.append(quote(fieldName));
              sql2.append("= ?");
              keyCount++;
            }
            else
            {
              // Create a 'SET' clause for non-key fields
              if (fieldCount>0)
              { sql1.append(",");
              }
              sql1.append(quote(fieldName));
              sql1.append("= ? ");
              _paramMap[count]=fieldCount+1;
              fieldCount++;
            }
            count++;
          }

          _statement=_connection.prepareStatement(sql1.toString()+sql2.toString());
          _connection.commit();
        }
        else if (_delete)
        {
          //
          // Prepare a DELETE
          //
          StringBuffer sql1=new StringBuffer();
          sql1.append("DELETE FROM "+quote(_tableName)+" WHERE ");

          int count=0;
          for (int i=0;i<numFields;i++)
          { 
            Field<?> field=fieldSet.getFieldByIndex(i);
            _typeMap[count]=translateType(field);
            String fieldName=field.getName();

            // Note the index of the key fields
            _paramMap[count]=count+1;
            if (count>0)
            { sql1.append(" AND ");
            }
            sql1.append(quote(fieldName));
            sql1.append("= ?");
            count++;
          }

          _statement=_connection.prepareStatement(sql1.toString());
          
        }
        else
        {
          //
          // Prepare an INSERT
          //
          StringBuffer sql1=new StringBuffer();
          StringBuffer sql2=new StringBuffer();
          sql1.append("INSERT INTO "+quote(_tableName)+" (");
          sql2.append(" VALUES (");
    
          int count=0;
          for (int i=0;i<numFields;i++)
          { 
            Field<?> field=fieldSet.getFieldByIndex(i);
            _typeMap[count]=translateType(field);
            
            if (count>0)
            { 
              sql1.append(",");
              sql2.append(",");
            }
            sql1.append(quote(field.getName()));
            sql2.append("?");
            _paramMap[count]=count+1;
            count++;
          }
          sql1.append(")");
          sql2.append(")");
          _statement=_connection.prepareStatement(sql1.toString()+sql2.toString());
        }

      }
      catch (SQLException x)
      { throw new RuntimeException(x);
      }
		}


    private String quote(String name)
    { return "\""+name+"\"";
    }
    
		public void performSql()
		{
      boolean notAllNull=false;
      if (insertKeyBinding!=null)
      {
        Tuple keyData=insertKeyBinding.get();
        if (_keyMap.get(keyData)!=null)
        { 
          log.warning("DUPLICATE KEY: "+keyData.toString());
          return;
        }
        else
        { _keyMap.put(keyData,keyData);
        }
      }
      
      try
      {
        if (_count>=_skipCount)
        {
          int fieldCount=0;
          for (Field<?> field: fieldSet.fieldIterable())
          {
            // XXX Use field bindings
            Object dataObject=field.getValue(dataGetTuple());
            if (_paramMap[fieldCount]==-1)
            { 
              fieldCount++;
              continue;
            }
            if (dataObject!=null)
            { 
              try
              {
                switch(_typeMap[fieldCount])
                {
                case Types.TIMESTAMP:
                  _statement
                    .setObject
                      (_paramMap[fieldCount]
                      ,new Timestamp
                        (_dateFormat.parse((String) dataObject).getTime()
                        )
                      );
                  break;
                case Types.DOUBLE:
                  if (dataObject instanceof String)
                  { dataObject=Double.valueOf((String) dataObject);
                  }
                  _statement.setDouble(_paramMap[fieldCount],((Number) dataObject).doubleValue());
                  break;
                case Types.NUMERIC:
                  if (dataObject instanceof String)
                  { dataObject=new BigDecimal((String) dataObject);
                  }
                  _statement.setBigDecimal(_paramMap[fieldCount],new BigDecimal( ((Number) dataObject).doubleValue()));
                  break;
                case Types.INTEGER:
                  if (dataObject instanceof String)
                  { dataObject=Integer.valueOf((String) dataObject);
                  }
                  _statement.setInt(_paramMap[fieldCount],((Number) dataObject).intValue());
                  break;
                case Types.FLOAT:
                  if (dataObject instanceof String)
                  { dataObject=Float.valueOf((String) dataObject);
                  }
                  _statement.setFloat(_paramMap[fieldCount],((Float) dataObject).floatValue());
                  break;
                case Types.LONGVARCHAR:
                  _statement.setObject(_paramMap[fieldCount],dataObject,_typeMap[fieldCount]);
                  break;
                case Types.LONGVARBINARY:
                  _statement.setBytes(_paramMap[fieldCount],(byte[]) dataObject);
                  break;
                case Types.BIT:
                  if (dataObject instanceof Boolean)
                  { 
                    _statement.setBoolean
                      (_paramMap[fieldCount],((Boolean) dataObject).booleanValue());
                  }
                  break;
                default:
                  _statement.setObject(_paramMap[fieldCount],dataObject);
                }
              }
              catch (java.text.ParseException x)
              { throw new RuntimeException("Error parsing '"+dataObject+"' in "+dataGetTuple().toString()+":"+x.toString());
              }
              catch (ClassCastException x)
              { throw new RuntimeException("Unexpected data type '"+dataObject.getClass()+"' found for '"+dataObject+"' in "+dataGetTuple().toString());
              }
              notAllNull=true;
            }
            else
            { _statement.setNull(_paramMap[fieldCount],_typeMap[fieldCount]);
            }
            fieldCount++;
          }

          if (notAllNull)
          {
            if (!_checkKey)
            {
              int count=_statement.executeUpdate();
              if (count<1)
              { 
                if (_discardWriter!=null)
                { _discardWriter.println(dataGetTuple().toString());
                }
                else
                { log.info(dataGetTuple().toString());
                }
              }
              _count++;
              if (_count%_statusInterval==0)
              { log.info("Sent "+_count+" rows");
              }
              if (_count%_transactionSize==0)
              { 
                _connection.commit();
                log.info("Updated "+_count);
              }
            }
            else
            {
              ResultSet rs=_statement.executeQuery();
              if (!rs.next())
              { throw new SQLException("Null result!");
              }
              int keyCount=rs.getInt(1);
              if (keyCount<1)
              { log.info("Missing "+dataGetTuple());
              }
              else if (keyCount>1)
              { log.info("Duplicated "+keyCount+"x "+dataGetTuple()); 
              }
              
              if (++_count%_transactionSize==0)
              { log.info("Queried "+_count);
              }
            }
          }
          else
          { log.info("Skipped null row "+_count);
          }
        }
        else
        { 
          if (++_count%_transactionSize==0)
          { log.info("Skipped "+_count);
          }
        }
      }
      catch (SQLException x)
      { 
        log.log
          (Level.SEVERE,"Caught SqlException processing "
            +dataGetTuple().toString()
          ,x
          );
        throw new RuntimeException(x.toString());
      }
      catch (DataException x)
      { 
        log.log(Level.SEVERE
          ,"Caught DataException processing "
            +dataGetTuple().toString()
          ,x
          );
        throw new RuntimeException(x.toString());
      }

		}

	}
		


}

