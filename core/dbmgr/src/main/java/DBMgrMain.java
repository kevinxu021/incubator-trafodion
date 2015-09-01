
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Properties;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.SecuredRedirectHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

public class DBMgrMain {

  static final String[] sslCipherIncludes = { "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
      "SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA",
      "SSL_RSA_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_DSS_WITH_AES_128_CBC_SHA",
      "SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
      "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256", "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
      "TLS_DHE_DSS_WITH_AES_256_CBC_SHA256", "TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA",
      "TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
      "TLS_RSA_WITH_CAMELLIA_256_CBC_SHA", "TLS_PSK_WITH_AES_256_CBC_SHA ",
      "TLS_RSA_WITH_3DES_EDE_CBC_SHA", "SSL_CK_DES_192_EDE3_CBC_WITH_MD5",
      "TLS_PSK_WITH_3DES_EDE_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
      "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256", "TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA",
      "TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA",
      "TLS_RSA_WITH_CAMELLIA_128_CBC_SHA", "TLS_PSK_WITH_AES_128_CBC_SHA",
      "TLS_DHE_RSA_WITH_SEED_CBC_SHA", "TLS_DHE_DSS_WITH_SEED_CBC_SHA", "TLS_RSA_WITH_SEED_CBC_SHA",
      "TLS_PSK_WITH_RC4_128_SHA" };

  static final String[] sslCipherExcludes =
      { "SSL_RSA_WITH_DES_CBC_SHA", "SSL_DHE_RSA_WITH_DES_CBC_SHA", "SSL_DHE_DSS_WITH_DES_CBC_SHA",
          "SSL_RSA_EXPORT_WITH_RC4_40_MD5", "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA",
          "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA", "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA",
          "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA", "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
          "SSL_RSA_WITH_DES_CBC_SHA" };

  public static void main(String[] args) {
    // TODO Auto-generated method stub

    int httpPort = 8080;
    int httpsPort = 8443;
    int requestHeaderSize = 96 * 1024;

    try {
      // File webAppFile = findWebAppDir();

      String DBMGR_HOME = System.getenv("DBMGR_INSTALL_DIR");
      if (DBMGR_HOME == null || DBMGR_HOME.isEmpty()) {
        System.out
.println("FATAL : DBMGR_INSTALL_DIR variable is not set.");
        System.exit(-1);

      }

      String configFileName = DBMGR_HOME + "/conf/config.xml";
      File configFile = new File(configFileName);
      if (!configFile.exists()) {
        System.out.println("FATAL : Cannot find DBMgr configuration file : " + configFileName);
        System.exit(-1);
      }

      Properties config = readDBMgrConfig(DBMGR_HOME + "/conf/config.xml");
      try {
        httpPort = Integer.parseInt(config.getProperty("httpPort", "8080"));
      } catch (Exception ex) {
        System.out
            .println("Cannot parse the httpPort property value as integer. "
            + ex.getMessage());
        System.exit(-1);
      }
      try {
        httpsPort = Integer.parseInt(config.getProperty("httpsPort", "8443"));
      } catch (Exception ex) {
        System.out
            .println("Cannot parse the httpsPort property value as integer. " + ex.getMessage());
        ;
      }
      try {
        requestHeaderSize = Integer.parseInt(config.getProperty("requestHeaderSize", "98304"));
      } catch (Exception ex) {
        System.out.println(
          "Cannot parse the requestHeaderSize property value as integer. "
            + ex.getMessage());
      }

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(500);
      Server server = new Server(threadPool);

      // Server server = new Server(8080);

      ProtectionDomain protectionDomain = DBMgrMain.class.getProtectionDomain();
      URL location = protectionDomain.getCodeSource().getLocation();

      ServletContextHandler ctx = new ServletContextHandler();
      ctx.setContextPath("/");

      DefaultServlet defaultServlet = new DefaultServlet();
      ServletHolder holderPwd = new ServletHolder("default", defaultServlet);
      holderPwd.setInitParameter("resourceBase", DBMGR_HOME + "/work/webapp/");
      ctx.addServlet(holderPwd, "/*");

      WebAppContext context = new WebAppContext();
      context.setContextPath("/resources/*");
      context.setTempDirectory(MakeTempDirectory(DBMGR_HOME + "/work"));
      context.setWar(location.toExternalForm());
      context.setParentLoaderPriority(true);
      context.setOverrideDescriptor(DBMGR_HOME + "/work/webapp/WEB-INF/classes/override-web.xml");

      HttpConfiguration http_config = new HttpConfiguration();
      http_config.setSecureScheme("https");
      http_config.setSecurePort(httpsPort);
      http_config.setOutputBufferSize(requestHeaderSize);
      http_config.setRequestHeaderSize(requestHeaderSize);
      http_config.setResponseHeaderSize(requestHeaderSize);
      http_config.setSendServerVersion(true);
      http_config.setSendDateHeader(false);

      ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(http_config));
      http.setPort(httpPort);
      http.setIdleTimeout(30000);
      server.addConnector(http);

      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(DBMGR_HOME + "/etc/dbmgr.keystore");
      sslContextFactory.setKeyStorePassword(config.getProperty("securePassword"));
      sslContextFactory.setKeyManagerPassword(config.getProperty("securePassword"));
      sslContextFactory.setTrustStorePath(DBMGR_HOME + "/etc/dbmgr.keystore");
      sslContextFactory.setTrustStorePassword(config.getProperty("securePassword"));
      sslContextFactory.setIncludeCipherSuites(sslCipherIncludes);
      sslContextFactory.setExcludeCipherSuites(sslCipherExcludes);

      // SSL HTTP Configuration
      HttpConfiguration https_config = new HttpConfiguration(http_config);
      https_config.addCustomizer(new SecureRequestCustomizer());

      // SSL Connector
      ServerConnector sslConnector = new ServerConnector(server,
          new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
          new HttpConnectionFactory(https_config));
      sslConnector.setPort(httpsPort);
      server.addConnector(sslConnector);

      ContextHandlerCollection contexts = new ContextHandlerCollection();
      contexts.setHandlers(new Handler[] { ctx, context });

      // Add a Handlers for requests
      HandlerList handlers = new HandlerList();
      handlers.addHandler(new SecuredRedirectHandler());
      // handlers.addHandler(ctx);
      handlers.addHandler(contexts);
      server.setHandler(handlers);

      // server.setHandler(contexts);
      server.start();
      server.dump(System.err);
      server.join();

    } catch (Exception ex) {
      System.out.println(ex.getMessage());
    }
  }

  private static File MakeTempDirectory(String sDirName) {
    File dir = null;

    try {
      dir = new File(sDirName);
      if (!dir.exists()) dir.mkdirs();
    } catch (Exception ex) {
      System.out.println("Failed to create " + sDirName + " : " + ex.getMessage());
    }

    return dir;
  }

  private static Properties readDBMgrConfig(String fileName) {
    Properties prop = new Properties();
    InputStream input = null;

    try {
      input = new FileInputStream(fileName);
      // load a properties file
      prop.loadFromXML(input);
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }
}
