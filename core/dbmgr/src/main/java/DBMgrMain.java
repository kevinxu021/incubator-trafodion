// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

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

	static final String[] sslCipherIncludes = { "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
			"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
			"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA",
			"TLS_RSA_WITH_AES_256_CBC_SHA256", "TLS_RSA_WITH_AES_256_CBC_SHA" };

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int httpPort = 4205;
		int httpsPort = 4206;
		int requestHeaderSize = 96 * 1024;

		try {
			// File webAppFile = findWebAppDir();
			ProtectionDomain protectionDomain = DBMgrMain.class.getProtectionDomain();
			URL location = protectionDomain.getCodeSource().getLocation();

			if (args.length > 0) {
				for (String arg : args) {
					if (arg.contains("-version")) {
						printVersion(location.getFile());
						System.exit(1);
					}
				}
			}

			String DBMGR_HOME = System.getenv("DBMGR_INSTALL_DIR");
			if (DBMGR_HOME == null || DBMGR_HOME.isEmpty()) {
				System.out.println(
						"FATAL : DBMGR_INSTALL_DIR variable is not set.");
				System.exit(-1);

			}

			String configFileName = DBMGR_HOME + "/conf/config.xml";
			File configFile = new File(configFileName);
			if (!configFile.exists()) {
				System.out.println(
						"FATAL : Cannot find DBMgr configuration file : "
								+ configFileName);
				System.exit(-1);
			}

			Properties config = readDBMgrConfig(configFileName);
			for (Object key : config.keySet()) {

				String overrideValue = System.getProperty((String) key);
				if (overrideValue != null) {
					config.put(key, overrideValue);
				}
			}

			try {
				httpPort = Integer.parseInt(config.getProperty("httpPort", String.valueOf(httpPort)));
			} catch (Exception ex) {
				System.out.println(
						"Cannot parse the httpPort property value as integer. "
								+ ex.getMessage());
				System.exit(-1);
			}
			try {
				httpsPort = Integer.parseInt(config.getProperty("httpsPort", String.valueOf(httpsPort)));
			} catch (Exception ex) {
				System.out.println(
						"Cannot parse the httpsPort property value as integer. "
								+ ex.getMessage());;
			}
			try {
				requestHeaderSize = Integer.parseInt(
						config.getProperty("requestHeaderSize", "98304"));
			} catch (Exception ex) {
				System.out.println(
						"Cannot parse the requestHeaderSize property value as integer. "
								+ ex.getMessage());
			}

			QueuedThreadPool threadPool = new QueuedThreadPool();
			threadPool.setMaxThreads(500);
			Server server = new Server(threadPool);

			// Server server = new Server(8080);

			ServletContextHandler ctx = new ServletContextHandler();
			ctx.setContextPath("/");

			DefaultServlet defaultServlet = new DefaultServlet();
			ServletHolder holderPwd = new ServletHolder("default",
					defaultServlet);
			holderPwd.setInitParameter("resourceBase",
					DBMGR_HOME + "/work/webapp/");
			ctx.addServlet(holderPwd, "/*");

			WebAppContext context = new WebAppContext();
			context.setContextPath("/resources/*");
			context.setTempDirectory(MakeTempDirectory(DBMGR_HOME + "/work"));
			context.setWar(location.toExternalForm());
			context.setParentLoaderPriority(true);
			context.setOverrideDescriptor(DBMGR_HOME
					+ "/work/webapp/WEB-INF/classes/override-web.xml");

			HttpConfiguration http_config = new HttpConfiguration();
			http_config.setSecureScheme("https");
			http_config.setSecurePort(httpsPort);
			http_config.setOutputBufferSize(requestHeaderSize);
			http_config.setRequestHeaderSize(requestHeaderSize);
			http_config.setResponseHeaderSize(requestHeaderSize);
			http_config.setSendServerVersion(true);
			http_config.setSendDateHeader(false);

			ServerConnector http = new ServerConnector(server,
					new HttpConnectionFactory(http_config));
			http.setPort(httpPort);
			http.setIdleTimeout(30000);
			server.addConnector(http);

			SslContextFactory sslContextFactory = new SslContextFactory();
			sslContextFactory
					.setKeyStorePath(DBMGR_HOME + "/etc/dbmgr.keystore");
			sslContextFactory
					.setKeyStorePassword(config.getProperty("securePassword"));
			sslContextFactory.setKeyManagerPassword(
					config.getProperty("securePassword"));
			sslContextFactory
					.setTrustStorePath(DBMGR_HOME + "/etc/dbmgr.keystore");
			sslContextFactory.setTrustStorePassword(
					config.getProperty("securePassword"));
			sslContextFactory.setIncludeCipherSuites(sslCipherIncludes);
			
			// SSL HTTP Configuration
			HttpConfiguration https_config = new HttpConfiguration(http_config);
			https_config.addCustomizer(new SecureRequestCustomizer());

			// SSL Connector
			ServerConnector sslConnector = new ServerConnector(server,
					new SslConnectionFactory(sslContextFactory,
							HttpVersion.HTTP_1_1.asString()),
					new HttpConnectionFactory(https_config));
			sslConnector.setPort(httpsPort);
			server.addConnector(sslConnector);

			ContextHandlerCollection contexts = new ContextHandlerCollection();
			contexts.setHandlers(new Handler[]{ctx, context});

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
			if (!dir.exists())
				dir.mkdirs();
		} catch (Exception ex) {
			System.out.println(
					"Failed to create " + sDirName + " : " + ex.getMessage());
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

	private static void printVersion(String jarFileName) {
		Attributes.Name productNameKey = new Attributes.Name("Product-Name");
		Attributes.Name versionKey = new Attributes.Name("Implementation-Version-2");
		Attributes.Name branchKey = new Attributes.Name("Implementation-Version-5");
		Attributes.Name buildDateKey = new Attributes.Name("Implementation-Version-6");
		JarFile jarFile = null;
		try {
			jarFile = new JarFile(jarFileName);
			Manifest manifest = jarFile.getManifest();
			Attributes mainAttr = manifest.getMainAttributes();

			if (mainAttr.containsKey(productNameKey)) {
				System.out.print(mainAttr.getValue(productNameKey) + " ");
			}
			if (mainAttr.containsKey(versionKey)) {
				System.out.print(mainAttr.getValue(versionKey) + " (");
			}
			if (mainAttr.containsKey(branchKey)) {
				System.out.print(mainAttr.getValue(branchKey) + ", ");
			}
			if (mainAttr.containsKey(buildDateKey)) {
				System.out.println(mainAttr.getValue(buildDateKey) + ")");
			}
		} catch (Exception ex) {
			System.out.println("Error reading jar file '" + jarFileName + "', error " + ex);
			System.exit(-1);
		} finally {
			if (jarFile != null) {
				try {
					jarFile.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
