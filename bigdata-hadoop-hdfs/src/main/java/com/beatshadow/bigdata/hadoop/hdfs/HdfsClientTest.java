package com.beatshadow.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hdfs 客户端测试
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/7/23 13:57
 */
public class HdfsClientTest {

    private static Logger logger = Logger.getLogger(HdfsClientTest.class);

    private FileSystem fileSystem ;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        System.setProperty("hadoop.home.dir","/Users/gnehcgnaw/Environments/hadoop-3.3.0");
        fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"root");
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
        logger.debug("fileSystem is close");
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void test1() throws IOException {
        fileSystem.mkdirs(new Path("/bigdata/hadoop/hdfs1"));
    }

    /**
     * 删除文件，及其文件夹
     */
    @Test
    public void test2() throws IOException {
        Path path = new Path("/");
        listStatusTest(path);
    }

    public void listStatusTest (Path path ) throws IOException {
        for (FileStatus fileStatus :  fileSystem.listStatus(path)) {
            if (fileStatus.isDirectory()&&((HdfsLocatedFileStatus)fileStatus).getChildrenNum()>0) {
                path = fileStatus.getPath() ;
                listStatusTest(path);
            }else {
                System.out.println(fileStatus.getPath());
                Path current = new Path(fileStatus.getPath().toUri().getPath());
                fileSystem.delete(current, false);
            }
        }
    }
}
