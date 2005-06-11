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
package spiralcraft.sql;

import java.net.URI;

import java.util.Properties;
/**
 * Information required by the DriverAgent to create a database connection
 */
public interface ConnectionInfo
{
  //XXX Implement change events
  
  public Class getDriverClass();
  
  public Class getVendorAdviceClass();
  
  public URI getDatabaseURL();
  
  public String getUser();
  
  public String getPassword();
  
  public Properties getProperties();
  
}