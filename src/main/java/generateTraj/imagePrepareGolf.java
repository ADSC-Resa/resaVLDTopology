package generateTraj;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;
import tool.Serializable;
import util.ConfigUtil;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static tool.Constants.*;

/**
 * Created by Tom Fu, July 30, 2015
 * Input is raw video frames of f_(i-1) and f_i, output gray scale of the two frames gray_f(i-1), gray_f(i),
 * Fix a bug in the Echo version,
 * It should be the Prev_frame to generate the new Trace, instead of the later one!!
 * Golf version 是基于Fox version， 实现一个新功能，按照Peiyong的建议，当输入文件的resolution很大的时候，可以先resize变小，产生trajectory之后，
 * 再显示的时候resize回来。
 *
 * Change compare to Fox version: 不需要专门输出一个Raw frame for Draw bolt了
 */
public class imagePrepareGolf extends BaseRichBolt {
    OutputCollector collector;

    IplImage image, prev_image, grey, prev_grey;
    IplImagePyramid grey_pyramid, prev_grey_pyramid;
    IplImagePyramid eig_pyramid;

    static int scale_num = 1;
    static float scale_stride = (float) Math.sqrt(2.0);
    static int ixyScale = 0;

    double min_distance;
    double quality;
    int init_counter;

    List<Integer> traceGeneratorTasks;
    String traceGeneratorName;

    public imagePrepareGolf(String traceGeneratorName){
        this.traceGeneratorName = traceGeneratorName;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.image = null;
        this.prev_image = null;
        this.grey = null;
        this.prev_grey = null;

        this.grey_pyramid = null;
        this.prev_grey_pyramid = null;

        this.min_distance = ConfigUtil.getDouble(map, "min_distance", 5.0);
        this.quality = ConfigUtil.getDouble(map, "quality", 0.001);
        this.init_counter = ConfigUtil.getInt(map, "init_counter", 1);

        traceGeneratorTasks = topologyContext.getComponentTasks(traceGeneratorName);

        IplImage imageFK = new IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);

        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        IplImage frame = sMat.toJavaCVMat().asIplImage();

        Serializable.Mat sMatPrev = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT_PREV);
        IplImage framePrev = sMatPrev.toJavaCVMat().asIplImage();

        Serializable.Mat sMatOrg = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT_ORG);
        collector.emit(ORIGINAL_FRAME_OUTPUT, tuple, new Values(frameId, sMatOrg));

        if (this.image == null || frameId == 1) { //only first time
            image = cvCreateImage(cvGetSize(frame), 8, 3);
            image.origin(frame.origin());

            prev_image = cvCreateImage(cvGetSize(frame), 8, 3);
            prev_image.origin(frame.origin());

            grey = cvCreateImage(cvGetSize(frame), 8, 1);
            grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            prev_grey = cvCreateImage(cvGetSize(frame), 8, 1);
            prev_grey_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(frame), 8, 1);

            ///a bug fixed here for Fox version!, use prev_grey_temp instead of grey_temp
            eig_pyramid = new IplImagePyramid(scale_stride, scale_num, cvGetSize(this.prev_grey), 32, 1);
        }

        cvCopy(frame, image, null);
        opencv_imgproc.cvCvtColor(image, grey, opencv_imgproc.CV_BGR2GRAY);
        grey_pyramid.rebuild(grey);
        IplImage grey_temp = cvCloneImage(grey_pyramid.getImage(ixyScale));
        Mat gMat = new Mat(grey_temp);
        Serializable.Mat sgMat = new Serializable.Mat(gMat);

        cvCopy(framePrev, prev_image, null);
        opencv_imgproc.cvCvtColor(prev_image, prev_grey, opencv_imgproc.CV_BGR2GRAY);
        prev_grey_pyramid.rebuild(prev_grey);
        IplImage prev_grey_temp = cvCloneImage(prev_grey_pyramid.getImage(ixyScale));
        Mat gMatPrev = new Mat(prev_grey_temp);
        Serializable.Mat sgMatPrev = new Serializable.Mat(gMatPrev);

        collector.emit(STREAM_GREY_FLOW, tuple, new Values(frameId, sgMat, sgMatPrev));

        int frameWidth = grey.width();
        int frameHeight = grey.height();
        int eigWidth = cvFloor(frameWidth / min_distance);
        int eigHeight = cvFloor(frameHeight / min_distance);
        int preFrameID = frameId -1 ;
        if (preFrameID % init_counter == 0) {

            IplImage eig_temp = cvCloneImage(eig_pyramid.getImage(ixyScale));
            double[] maxVal = new double[1];
            maxVal[0] = 0.0;
            ///a bug fixed here for Fox version!, use prev_grey_temp instead of grey_temp
            opencv_imgproc.cvCornerMinEigenVal(prev_grey_temp, eig_temp, 3, 3);

            cvMinMaxLoc(eig_temp, null, maxVal, null, null, null);
            double threshold = maxVal[0] * quality;
            int offset = cvFloor(min_distance / 2.0);

            List<List<float[]>> group = new ArrayList<>();

            for (int i = 0; i < traceGeneratorTasks.size(); i++) {
                List<float[]> subGroup = new ArrayList<>();
                group.add(subGroup);
            }

            int floatArraySize = grey.width() + offset  + 1;
            for (int i = 0; i < eigHeight; i++) {
                int y = opencv_core.cvFloor(i * min_distance + offset);

                FloatBuffer floatBuffer =  eig_temp.getByteBuffer(y * eig_temp.widthStep()).asFloatBuffer();
                float[] floatArray = new float[floatArraySize];
                floatBuffer.get(floatArray);

                int index = i % traceGeneratorTasks.size();
                group.get(index).add(floatArray);
            }

            for (int i = 0; i < traceGeneratorTasks.size(); i++) {
                int tID = traceGeneratorTasks.get(i);
                //System.out.println("i: " + i + ", tID: " + tID + ", size: " + group.get(i).size() + ",w: "+ width + ", h: " + height + ",off: " + offset + ", min_dis:" + min_distance);
                collector.emitDirect(tID, STREAM_EIG_FLOW, tuple, new Values(preFrameID, group.get(i), new EigRelatedInfo(frameWidth, frameHeight, offset, threshold)));
            }
            cvReleaseImage(eig_temp);
        }
        cvReleaseImage(prev_grey_temp);
        cvReleaseImage(grey_temp);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_GREY_FLOW, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_FRAME_MAT_PREV));
        outputFieldsDeclarer.declareStream(STREAM_EIG_FLOW, true, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_EIG_INFO));
        outputFieldsDeclarer.declareStream(ORIGINAL_FRAME_OUTPUT, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT_ORG));
    }
}
