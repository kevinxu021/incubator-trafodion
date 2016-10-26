package org.trafodion.ci.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;

public class TrafLoader extends Thread {

	private Properties config;
	private volatile int count = 0;
	private volatile long rowCount = 0;
	private long startTime;
	private int totalFiles = 0;
	private TrafTargetTable targetTable;

	public TrafLoader(Properties config) {
		this.config = config;
		this.startTime = System.currentTimeMillis();

	}

	public static void main(String[] args) throws ClassNotFoundException, ParseException, FileNotFoundException, IOException {
		Properties p = new Properties();
		Options options = new Options();
		options.addOption("f", true, "Specify a configuration file.");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		if(cmd.hasOption("f")){
			p.load(new FileInputStream(cmd.getOptionValue("f")));
		}else{
			p.put("with_head", "false");
			p.put("target_table", "SUNNYOPT.QNT");
			p.put("source_path", "d:\\test");
			p.put("delimiter", "~");
			p.put("batch_size", "5000");
			p.put("max_thread_size", "3");
			int max_thread_size = Integer.parseInt(p.getProperty("max_thread_size", "3"));
			p.put("user", "zz");
			p.put("pwd", "zz");
			p.put("url", "jdbc:t4jdbc://10.10.10.8:23400/:maxPoolSize=" + max_thread_size);
		}

		new TrafLoader(p).start();
	}

	@Override
	public void run() {
		final String type = config.getProperty("type", "txt|csv");

		String source_path = config.getProperty("source_path", "d:\\test");
		int max_thread_size = Integer.parseInt(config.getProperty("max_thread_size", "3"));

		ExecutorService threadPool = Executors.newFixedThreadPool(max_thread_size);
		// got all files from specified folder
		Collection<File> files = FileUtils.listFiles(new File(source_path), new SuffixFileFilter(type.split("\\|")),
				DirectoryFileFilter.DIRECTORY);
		this.totalFiles = files.size();
		if (this.totalFiles == 0) {
			System.out.println("No files!");
			return;
		}
		try {
			this.targetTable = new TrafTargetTable(config);
		} catch (SQLException e1) {
			e1.printStackTrace();
			return;
		}
		Iterator<File> it = files.iterator();
		// read from file line by line
		while (it.hasNext())
			threadPool.submit(new TrafFileExecutor(this, it.next(), config, this.targetTable));

		threadPool.shutdown();
		try {
			threadPool.awaitTermination(10, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("All done!");
	}

	/**
	 * @param processedFile
	 *            Processed file just now.
	 * @param rowcount
	 *            The number of rows has been retrieved.
	 * @param elapse
	 *            Elapsed time in milliseconds.
	 */
	public synchronized void countDown(String threadName, File processedFile, long rowCnt, long elapse) {
		++this.count;
		this.rowCount += rowCnt;
		System.out
				.println(String.format("[%s] Rows: %d. Elapse: %s. Rows per second: %d. File: %s", threadName, rowCnt,
						toElapseString(elapse), toPerSecString(rowCnt, elapse), processedFile.getAbsolutePath()));
		long totalEls = System.currentTimeMillis() - this.startTime;
		System.out.println(
				String.format("[Total] Rows: %d. Elapse: %s. Rows per second: %d. Progress: %d/%d. %d%%", this.rowCount,
						toElapseString(totalEls), toPerSecString(this.rowCount, totalEls), this.count, this.totalFiles,
						this.count * 100 / this.totalFiles));
	}

	private long toPerSecString(long rowCnt, long elapse) {
		return elapse / 1000 == 0 ? 0 : rowCnt / (elapse / 1000);
	}

	private String toElapseString(long elapse) {
		long seconds = (elapse / 1000) % 60;
		long minutes = (seconds / 60) % 60;
		long hours = minutes / 60;
		return String.format("%dh %dm %ds", hours, minutes, seconds);
	}

}
