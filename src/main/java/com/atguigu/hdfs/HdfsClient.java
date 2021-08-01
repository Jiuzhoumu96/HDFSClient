package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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
     *
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
        String user = "atguigu";
        //获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**
     * 本地文件上传到HDFS
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml=> 在项目资源目录下的配置文件 => 代码里面的配置
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        //参数解读：参数一：是否删除源数据；参数二：是否允许覆盖;参数三：源数据路径；参数四：目的路径
        fs.copyFromLocalFile(false, true, new Path("E:\\LinuxSystem\\CentOs7\\tempfiles\\sunwukong.txt"), new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    /**
     * HDS文件下载到本地
     *
     * @throws IOException
     */
    @Test
    public void testGet() throws IOException {
        //参数解读：参数一：是否删除源数据；参数二：源数据路径;参数三：目的路径；参数四：是否允开启本地校验
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyou/huaguoshan/sunwukong.txt"), new Path("E:\\LinuxSystem\\CentOs7\\tempfiles\\sunwukongBackup.txt"), true);
    }

    /**
     * HDFS文件删除
     *
     * @throws IOException
     */
    @Test
    public void testRm() throws IOException {

        // 参数解读： 参数 1：要删除的路径； 参数2：是否递归删除
        // 删除文件
        // fs.delete(new Path("/xiyou/huaguoshan"),false);

        // 删除空目录
        //  fs.delete(new Path("/xiyou/huaguoshan"),false);

        // 删除非空目录
        fs.delete(new Path("/xiyou/huaguoshan"), true);

    }

    /**
     * 文件的更名和移动
     *
     * @throws IOException
     */
    @Test
    public void testMv() throws IOException {
        // 参数解读： 参数1：原文件路径； 参数2：目标文件路径
        // 对文件名称的修改
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("/xiyou/huaguoshan/sunwukong2.txt"));

        // 对文件的移动和更名
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("/xiyou/sunwukong2.txt"));

        // 目录的更名
        fs.rename(new Path("/input"), new Path("/output"));
    }

    /**
     * 获取文件详情
     *
     * @throws IOException
     */
    @Test
    public void fileDetail() throws IOException {

        // 获取所以文件信息
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println("======" + locatedFileStatus.getPath() + "======");
            System.out.println(locatedFileStatus.getPermission());
            System.out.println(locatedFileStatus.getOwner());
            System.out.println(locatedFileStatus.getGroup());
            System.out.println(locatedFileStatus.getLen());
            System.out.println(locatedFileStatus.getModificationTime());
            System.out.println(locatedFileStatus.getReplication());
            System.out.println(locatedFileStatus.getBlockSize());
            System.out.println(locatedFileStatus.getPath().getName());
            // 获取块信息
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }

    }

    /**
     * 判断是文件还是目录
     *
     * @throws IOException
     */
    @Test
    public void testFile() throws IOException {

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件" + fileStatus.getPath().getName());
            }
            if (fileStatus.isDirectory()) {
                System.out.println("目录" + fileStatus.getPath().getName());
            }
        }
    }
}
