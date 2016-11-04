package org.trafodion.ci.executor;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hive.hplsql.Proc;

public class ObeyExecutor implements GenericExecutor {

	private String obeyFileName;
	private Connection conn;
	private String url;
	private String clazzName;
	private String mdTableName;
	private String user;
	private String pwd;

	public ObeyExecutor(Connection conn, String server, String user, String pwd,
			String port,
			String obeyFileName) {
		this.obeyFileName = obeyFileName;
		this.conn = conn;
		if (port.contains(":")) {
			port = port.substring(port.indexOf(":") + 1);
		}
		this.url = "jdbc:t4jdbc://" + server + ":" + port + "/:";
		this.clazzName = "org.trafodion.jdbc.t4.T4Driver";
		this.user = user;
		this.pwd = pwd;
	}

	@Override
	public boolean matched() {
		LineIterator it = null;
		try {
			if (this.obeyFileName == null)
				return false;
			File file = new File(obeyFileName);
			if (!file.exists()) {
				return false;
			}
			it = FileUtils.lineIterator(file);
			if (it.hasNext()) {
				String line = it.next();
				return line.matches("\\s*/[*]\\s*trafodion\\s+plsql\\s*[*]/\\s*");
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			if (it != null) {
				it.close();
			}
		}
		return false;
	}

	@Override
	public void run() {
		Proc.exec(this.conn, new File(obeyFileName), this.mdTableName, clazzName, url, user, pwd);
	}

}
