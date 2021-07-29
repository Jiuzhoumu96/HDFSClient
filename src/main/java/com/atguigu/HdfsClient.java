package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Jiuzhoumu
 * @date 2021/4/12 19:50
 * @projectName HDFSClient
 * @title: HdfsClient
 * @description: TODO
 */
public class HdfsClient {

    private static FileSystem fs;

    /**
     * 初始化
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn的地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();
        //用户
        String user ="atguigu";
        //获取到了客户端对象
        fs = FileSystem.get(uri, configuration,user);
    }

    /**
     * 关闭连接
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 创建目录
     * @throws IOException
     */
    @Test
    public void testmkdir() throws IOException {
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     *
     * @throws IOException
     */
    @Test
    public void testput() throws IOException {
        //参数解读：参数一：是否删除源数据；参数二：是否允许覆盖;参数三：源数据路径；参数四：目的路径
        fs.copyFromLocalFile(false,true,new Path("E:\\LinuxSystem\\CentOs7\\tempfiles\\sunwukong.txt"),new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

}
