package server;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FrameGrabber;
import redis.clients.jedis.Jedis;
import tool.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.io.Serializable;

/**
 * Created by Ian.
 */

public class TomVideoStreamToFile {

    private String host;
    private int port;
    private byte[] queueName;
    private Jedis jedis = null;

    public TomVideoStreamToFile(String host, int port, String queueName) {
        this.host = host;
        this.port = port;
        this.queueName = queueName.getBytes();
    }

    public void VideoStreamReceiver() throws IOException, FrameGrabber.Exception, InterruptedException {

        opencv_core.IplImage fk = new opencv_core.IplImage();

        Jedis jedis = getConnectedJedis();
        byte[] baData = null;
        int x = 0;
        long ts = System.currentTimeMillis();
        while (true) {
            try {

                baData = jedis.lpop(queueName);

                if (baData != null) {
                    tool.Serializable.Mat sMat = new tool.Serializable.Mat(baData);
                    opencv_core.Mat mat = sMat.toJavaCVMat();
                    BufferedImage bufferedImageRead = mat.getBufferedImage();
                    //BufferedImage bufferedImageRead = ImageIO.read(new ByteArrayInputStream(baData));
                    ImageIO.write(bufferedImageRead, "JPEG", new File("C:\\Users\\Ian\\Desktop\\output\\" + x + ".jpg"));
                    x++;
                    System.out.println(x);
                }
                else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie)
                    {}
                }
            } catch (Exception e) {
                e.printStackTrace();
                disconnect();
            }
        }
    }

    public static void main(String args[]) {
        if (args.length < 3) {
            System.out.println("usage: TomVideoStreamToFile <Redis host> <Redis port> <Redis Queue>");
            return;
        }

        TomVideoStreamToFile tvsr = new TomVideoStreamToFile(args[0], Integer.parseInt(args[1]), args[2]);

        try {
            tvsr.VideoStreamReceiver();
        } catch (Exception e) {
        }
    }

    private Jedis getConnectedJedis() {
        if (jedis != null) {
            return jedis;
        }
        //try connect to redis server
        try {
            jedis = new Jedis(host, port);
        } catch (Exception e) {
        }
        return jedis;
    }

    private void disconnect() {
        try {
            jedis.disconnect();
        } catch (Exception e) {
        }
        jedis = null;
    }


}
