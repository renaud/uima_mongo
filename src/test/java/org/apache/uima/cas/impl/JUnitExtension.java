package org.apache.uima.cas.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

import junit.framework.Assert;

/**
 * Contains static convenience methods for using the UIMA JUnit extensions.
 * 
 */
public class JUnitExtension {

  public static File getFile(String aRelativeFilePath) {
    URL url = JUnitExtension.class.getClassLoader().getResource(aRelativeFilePath);
    File file = null;
    if (url != null) {
      try {
        String fileURL = URLDecoder.decode(url.getFile(), "UTF-8");
        file = new File(fileURL);
      } catch (UnsupportedEncodingException ex) {
        return null;
      }
    }
    return file;
  }

  public static URL getURL(String aRelativeFilePath) {
    return JUnitExtension.class.getClassLoader().getResource(aRelativeFilePath);
  }

  public static void handleException(Exception e) throws Exception {
    // check command line setting
    if (System.getProperty("isCommandLine", "false").equals("true")) {
      // print exception
     // ExceptionPrinter.printException(e);
      Assert.fail(e.getMessage());
    } else {
      // thow exception to the JUnit framework
      throw e;
    }
  }
}
