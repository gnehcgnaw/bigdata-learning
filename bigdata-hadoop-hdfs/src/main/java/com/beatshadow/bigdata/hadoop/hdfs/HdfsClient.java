package com.beatshadow.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Hdfs 客户端测试
 * @author : <a href="mailto:gnehcgnaw@gmail.com">gnehcgnaw</a>
 * @since : 2020/7/23 13:57
 */
public class HdfsClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.211.55.102:9000"),new Configuration(),"root");
        fileSystem.mkdirs(new Path("/bigdata/hadoop/hdfs1"));
        fileSystem.close();
        System.out.println("over");
    }
}
