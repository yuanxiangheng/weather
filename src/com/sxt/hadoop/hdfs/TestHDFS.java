package com.sxt.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFS {
	Configuration conf = null;
	FileSystem fs = null;

	@Before
	public void conn() throws Exception {
		conf = new Configuration(true);
		fs = FileSystem.get(conf);
	}

	@After
	public void close() throws Exception {
		fs.close();
	}

	@Test
	public void mkdidr() throws Exception {

		Path dirs = new Path("/data");
		if (fs.exists(dirs)) {
			fs.delete(dirs, true);
		}
		fs.mkdirs(dirs);

	}

	@Test
	public void upload() throws Exception {

		InputStream input = new BufferedInputStream(
				new FileInputStream(new File("E:\\BaiduNetdiskDownload\\尚学堂\\01LINUX\\高并发\\PPT\\nginx.pptx")));

		Path outFile = new Path("/data/ox.txt");
		FSDataOutputStream output = fs.create(outFile);
		IOUtils.copyBytes(input, output, conf, true);
	}

	@Test
	public void bloks() throws Exception {

		Path f = new Path("/data/ox.txt");
		FileStatus file = fs.getFileStatus(f);
		BlockLocation[] blks = fs.getFileBlockLocations(file, 0, file.getBlockSize());
		for (BlockLocation b : blks) {

			System.out.println(b);
		}
	}

}
