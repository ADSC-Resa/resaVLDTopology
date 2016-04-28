package tool;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import org.bytedeco.javacpp.opencv_objdetect;
import org.json.simple.*;
import org.json.simple.parser.*;
import redis.clients.jedis.Jedis;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getString;

/**
 * Created by Ian.
 */
public class CommandManagerSpout extends RedisQueueSpout {
    private String stormToNode;
    private byte[] logoData;

    public CommandManagerSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        System.out.println("Fake Image 1A");
        opencv_core.Mat mat = opencv_highgui.imread(tool.Constants.FAKE_IMAGE, opencv_highgui.CV_LOAD_IMAGE_COLOR);

        System.out.println("Fake Image 1B");
        opencv_objdetect.HOGDescriptor hog2 = new opencv_objdetect.HOGDescriptor(
                new opencv_core.Size(64, 64),
                new opencv_core.Size(16, 16),
                new opencv_core.Size(8, 8),
                new opencv_core.Size(8, 8),
                9
        );

        System.out.println("Fake Image 1C");
        hog2.compute(mat, new FloatPointer());

        System.out.println("Fake Image 1D - END");


        super.open(conf, context, collector);



        stormToNode = getString(conf, "slm.stormToNode", "stormToNode");
        logoData = getString(conf, "slm.logoData", "logoData").getBytes();
    }

    @Override
    protected void emitData(Object data) {
        JSONObject json;
        try {
            json = (JSONObject)new JSONParser().parse((String)data);
        } catch (ParseException e) {
            System.err.println("Error parsing command (JSONParser).");
            return;
        }

        String command = (String)json.get("command");
        int commandId = ((Long)json.get("id")).intValue();

        System.out.println("CM: " + command + ", " + commandId);

        switch (command) {
            case "add":
                processDataCommand(commandId, (String) json.get("name"), (String) data);
                break;
            case "delete":
                collector.emit(SLM_STREAM_DELETE_COMMAND, new Values(commandId, json.get("name")), (String) data);
                break;
            case "mute":
                collector.emit(SLM_STREAM_MUTE_COMMAND, new Values(commandId, json.get("name")), (String) data);
                break;
            case "unmute":
                collector.emit(SLM_STREAM_UNMUTE_COMMAND, new Values(commandId, json.get("name")), (String) data);
                break;
            case "setAlgorithm":
                collector.emit(SLM_STREAM_SET_ALGORITHM_COMMAND, new Values(commandId, json.get("sift"), json.get("hog")), (String) data);
                break;
            case "setDrawRectangles":
                collector.emit(SLM_STREAM_SET_DRAW_RECTANGLES_COMMAND, new Values(commandId, json.get("draw")), (String) data);
                break;
            case "setNMSSIFT":
                collector.emit(SLM_STREAM_SET_NMS_SIFT_COMMAND, new Values(commandId, json.get("nms")), (String) data);
                break;
            case "setNMSHOG":
                collector.emit(SLM_STREAM_SET_NMS_HOG_COMMAND, new Values(commandId, json.get("nms")), (String) data);
                break;
        }
    }

    private void processDataCommand(int commandId, String name, String data) {
        Jedis jedis = getConnectedJedis();
        if (jedis == null) {
            System.out.println("Null Jedis");
            return;
        }
        byte[] rawLogoData;
        try {
            rawLogoData = jedis.hget(logoData, name.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception in getting logo data.");
            return;
        }

        BufferedImage originalImage;
        try {
            originalImage = ImageIO.read(new ByteArrayInputStream(rawLogoData));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Exception in processing logo data.");
            return;
        }

        opencv_core.Mat originalMat = new opencv_core.Mat(opencv_core.IplImage.createFrom(originalImage));
        Serializable.Mat mat = new Serializable.Mat(originalMat);

        collector.emit(SLM_STREAM_ADD_COMMAND, new Values(commandId, name, mat), (String) data);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SLM_STREAM_ADD_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID, SLM_FIELD_RAW_LOGO_MAT));
        declarer.declareStream(SLM_STREAM_DELETE_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID));
        declarer.declareStream(SLM_STREAM_MUTE_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID));
        declarer.declareStream(SLM_STREAM_UNMUTE_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID));
        declarer.declareStream(SLM_STREAM_SET_ALGORITHM_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_USE_SIFT, SLM_FIELD_USE_HOG));
        declarer.declareStream(SLM_STREAM_SET_DRAW_RECTANGLES_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_DRAW));
        declarer.declareStream(SLM_STREAM_SET_NMS_SIFT_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_NMS));
        declarer.declareStream(SLM_STREAM_SET_NMS_HOG_COMMAND, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_NMS));
    }

    @Override
    public void ack(Object messageId) {
        Jedis jedis = getConnectedJedis();
        if (jedis == null) {
            System.out.println("Null Jedis");
            return;
        }
        try {
            jedis.lpush(stormToNode, (String)messageId);
        } catch (Exception e) {
            System.out.println("Exception in pushing reply.");
            return;
        }
    }

    @Override
    public void fail(Object messageId) {
        emitData(messageId); // Since the messageId is the raw JSON string itself... we just send it back out.
    }
}
