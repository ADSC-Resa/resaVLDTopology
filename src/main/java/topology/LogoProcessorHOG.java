package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.HOGDetector;
import org.bytedeco.javacpp.*;
import tool.Serializable;

import java.util.Map;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static tool.Constants.*;
import static util.ConfigUtil.*;

/**
 * Created by Ian.
 */
public class LogoProcessorHOG extends BaseRichBolt {
    OutputCollector collector;

    private int maxWidth, maxHeight;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("Fake Image 1A");
        opencv_core.Mat mat = opencv_highgui.imread("C:\\Users\\Ian\\Desktop\\FYP2\\hog\\1.jpg", opencv_highgui.CV_LOAD_IMAGE_COLOR);

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
        System.out.println("Initializing fake image - " + this.getClass().getCanonicalName());
        opencv_core.IplImage fakeImage = opencv_highgui.cvLoadImage("C:\\Users\\Ian\\Desktop\\FYP2\\fakeImage.png");
        System.out.println("Done initializing fake image - " + this.getClass().getCanonicalName());

        this.collector = outputCollector;

        maxWidth = (int)(getInt(map, "width", 640) * getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionX", 0.5));
        maxHeight = (int)(getInt(map, "height", 480) * getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionY", 0.5));
    }

    @Override
    public void execute(Tuple tuple) {
//        byte[] originalImageByteArray = tuple.getBinaryByField(SLM_FIELD_RAW_LOGO_MAT);
//        BufferedImage originalImage;
//        try {
//            originalImage = ImageIO.read(new ByteArrayInputStream(originalImageByteArray));
//        } catch (IOException e) {
//            e.printStackTrace();
//            collector.fail(tuple);
//            return;
//        }
//
        System.out.println("Test Route 2A");
        System.out.println("Getting sMat HOG");
        Serializable.Mat mat = (Serializable.Mat)tuple.getValueByField(SLM_FIELD_RAW_LOGO_MAT);
        opencv_core.Mat originalMat = mat.toJavaCVMat();//new opencv_core.Mat(opencv_core.IplImage.createFrom(originalImage));

        System.out.println("Test Route 2B");
        HOGDetector hogDetector = new HOGDetector(originalMat, maxWidth, maxHeight, 1.18);

        System.out.println("Test Route 2C");
        collector.emit(
                SLM_STREAM_ADD_PROCESSED_HOG_LOGO,
                tuple,
                new Values(tuple.getIntegerByField(SLM_FIELD_COMMAND_ID), tuple.getStringByField(SLM_FIELD_LOGO_ID) , hogDetector)
        );

        System.out.println("Test Route 2D");
        collector.ack(tuple);

        System.out.println("Test Route 2E - End");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SLM_STREAM_ADD_PROCESSED_HOG_LOGO, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID, SLM_FIELD_PROCESSED_HOG_LOGO));
    }
}
