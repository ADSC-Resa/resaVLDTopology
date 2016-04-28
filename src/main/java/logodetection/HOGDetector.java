package logodetection;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.bytedeco.javacpp.*;
import tool.Serializable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by Ian.
 */
public class HOGDetector implements KryoSerializable {
    private ArrayList<Integer> widths = new ArrayList<>();
    private ArrayList<Integer> heights = new ArrayList<>();
    private ArrayList<FloatPointer> descriptors = new ArrayList<>();

    public HOGDetector() {
        System.out.println("Initializing fake image - " + this.getClass().getCanonicalName());
        opencv_core.IplImage fakeImage2 = new opencv_core.IplImage();
        opencv_core.IplImage fakeImage = opencv_highgui.cvLoadImage(tool.Constants.FAKE_IMAGE);
        opencv_core.Mat fakeMat = opencv_highgui.imread(tool.Constants.FAKE_IMAGE, opencv_highgui.CV_LOAD_IMAGE_COLOR);
        System.out.println("Done initializing fake image - " + this.getClass().getCanonicalName());

        widths = new ArrayList<>();
        heights = new ArrayList<>();
        descriptors = new ArrayList<>();
    }

    public HOGDetector(opencv_core.Mat template, int maxWidth, int maxHeight, double resizeFactor) {
        this();

        opencv_core.Size maxSize = new opencv_core.Size(template.cols(), template.rows());
        if(maxSize.width() > maxWidth) {
            maxSize.height((int)Math.round((double)maxWidth/maxSize.width() * maxSize.height()));
            maxSize.width(maxWidth);
        }
        if(maxSize.height() > maxHeight) {
            maxSize.width((int)Math.round((double)maxHeight/maxSize.height() * maxSize.width()));
            maxSize.height(maxHeight);
        }
        //Solve rounding errors
        if(maxSize.width() > maxWidth) {
            maxSize.width(maxWidth);
        }
        if(maxSize.height() > maxHeight) {
            maxSize.height(maxHeight);
        }

        opencv_objdetect.HOGDescriptor hog = new opencv_objdetect.HOGDescriptor(
                new opencv_core.Size(64, 64),
                new opencv_core.Size(16, 16),
                new opencv_core.Size(8, 8),
                new opencv_core.Size(8, 8),
                9
        );

        double currentResizeFactor = 1.0;
        while(true) {
            int currentWidth = (int)Math.round(maxSize.width() / currentResizeFactor);
            int currentHeight = (int)Math.round(maxSize.height() / currentResizeFactor);
            int croppedWidth = currentWidth / 8 * 8; // Set to block size
            int croppedHeight = currentHeight / 8 * 8; // Set to block size

            int xOffset = (currentWidth - croppedWidth) / 2;
            int yOffset = (currentHeight - croppedHeight) / 2;

            if(croppedWidth < 64 || croppedHeight < 64)
                break;

            opencv_core.Mat resizedTemplate = new opencv_core.Mat();

            opencv_imgproc.resize(
                    template,
                    resizedTemplate,
                    new opencv_core.Size(currentWidth, currentHeight)
            );

            opencv_core.Rect roi = new opencv_core.Rect();
            roi.x(xOffset);
            roi.width(croppedWidth);
            roi.y(yOffset);
            roi.height(croppedHeight);

			System.out.println(xOffset + " " + yOffset + " " + currentWidth + " " + currentHeight);
			System.out.println(xOffset + " " + yOffset + " " + croppedWidth + " " + croppedHeight);

            opencv_core.Mat croppedTemplateTmp = new opencv_core.Mat(resizedTemplate, roi);
            opencv_core.Mat croppedTemplate = croppedTemplateTmp.clone();
//            croppedTemplateTmp.copyTo(croppedTemplate);
//			System.out.println(croppedTemplate.cols() + " " + croppedTemplate.rows());
//			opencv_core.IplImage image = new opencv_core.IplImage();
//			image = croppedTemplate.asIplImage();
//			cvShowImage("Video logo detection - single machine - offline", image);
//			int c = cvWaitKey(1000);

            FloatPointer descriptor = new FloatPointer();

            hog.winSize(new opencv_core.Size(croppedWidth, croppedHeight));

            System.out.println("HOGD_CP0");
            System.out.println("Initializing fake image (Internal) - " + this.getClass().getCanonicalName());
            opencv_core.IplImage fakeImage2 = new opencv_core.IplImage();
            opencv_core.IplImage fakeImage = opencv_highgui.cvLoadImage(tool.Constants.FAKE_IMAGE);
            opencv_core.Mat fakeMat = opencv_highgui.imread(tool.Constants.FAKE_IMAGE, opencv_highgui.CV_LOAD_IMAGE_COLOR);
            System.out.println("Done initializing fake image (Internal) - " + this.getClass().getCanonicalName());

            System.out.println("HOGD_CP1 " + croppedTemplate.cols() + " " + croppedTemplate.rows());
            hog.compute(croppedTemplate, descriptor);
            System.out.println("HOGD_CP2 " + descriptor.capacity());

//			int descriptorSize = (croppedTemplate.cols() / 8 - 1) * (croppedTemplate.rows() / 8 - 1) * 4 * 9;
//			System.out.println(descriptorSize + " " + descriptor.capacity());

            widths.add(croppedWidth);
            heights.add(croppedHeight);
            descriptors.add(descriptor);

            currentResizeFactor *= resizeFactor;
        }
    }

    public List<Serializable.ScoredRect> match(opencv_core.Mat image, double similarityCutoff) {
        return match(image, similarityCutoff, Integer.MAX_VALUE);
    }

    public List<Serializable.ScoredRect> match(opencv_core.Mat image, double similarityCutoff, int maxMatch) {
        int n = widths.size();
        List<Serializable.ScoredRect> hogFoundRectList = new ArrayList<>();
        PriorityQueue<Serializable.ScoredRect> pq = new PriorityQueue<>();

        opencv_objdetect.HOGDescriptor hog = new opencv_objdetect.HOGDescriptor(
                new opencv_core.Size(64, 64),
                new opencv_core.Size(16, 16),
                new opencv_core.Size(8, 8),
                new opencv_core.Size(8, 8),
                9
        );

        for(int i = 0; i < n; i++) {
            int width = widths.get(i);
            int height = heights.get(i);
            FloatPointer f1 = descriptors.get(i);

            hog.winSize(new opencv_core.Size(width, height));

            if(image.cols() < width || image.rows() < height)
                continue;

            FloatPointer f2 = new FloatPointer();
            hog.compute(image, f2);

            int descriptorSize = f1.capacity();
            int colOffset = ((image.cols() - width) / 8 + 1) * descriptorSize;

            for(int j = 0; j < f2.capacity(); j += f1.capacity()) {
                double v = calculateCosineDistance(f1, f2, f1.capacity(), 0, j);
                if(v > similarityCutoff) {
                    int row = j / colOffset * 8;
                    int col = (j % colOffset) / descriptorSize * 8;

                    Serializable.ScoredRect r = new Serializable.ScoredRect(col, row, width, height, v);
                    pq.add(r);
                }
            }

            f2.deallocate();
        }

        while(!pq.isEmpty() && hogFoundRectList.size() < maxMatch)
            hogFoundRectList.add(pq.poll());

        return hogFoundRectList;
    }

    private static double calculateCosineDistance(FloatPointer f1, FloatPointer f2) {
        if (f1.capacity() != f2.capacity()){
            throw new RuntimeException("Vector lengths must match!");
        }

        return calculateCosineDistance(f1, f2, f1.capacity(), 0, 0);
    }

    private static double calculateCosineDistance(FloatPointer f1, FloatPointer f2, int length, int offset1, int offset2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;

        for (int i = 0; i < length; i++) {
            double ff1 = 0.0;
            try {
                ff1 = f1.get(i + offset1);
            } catch(NullPointerException e) {
                System.out.println("Err");
                System.out.println(f1.capacity());
                System.out.println(f2.capacity());
                System.out.println(length);
                System.out.println(offset1);
                System.out.println(offset2);
                throw e;
            }
            double ff2 = f2.get(i + offset2);
            dotProduct += ff1 * ff2;
            magnitude1 += ff1 * ff1;
            magnitude2 += ff2 * ff2;
        }

        magnitude1 = Math.sqrt(magnitude1);
        magnitude2 = Math.sqrt(magnitude2);

        if (magnitude1 != 0.0 && magnitude2 != 0.0)
            return dotProduct / (magnitude1 * magnitude2);
        return 0.0;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        int n = widths.size();
        output.writeInt(n);

        for(int i = 0; i < n; i++) {
            output.writeInt(widths.get(i));
            output.writeInt(heights.get(i));

            FloatPointer fp = descriptors.get(i);
            int fpSize = fp.capacity();

            output.writeInt(fpSize);

            for(int j = 0; j < fpSize; j++)
                output.writeFloat(fp.get(j));
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        widths = new ArrayList<>();
        heights = new ArrayList<>();
        descriptors = new ArrayList<>();

        int n = input.readInt();

        for(int i = 0; i < n; i++) {
            int width = input.readInt();
            int height = input.readInt();

            widths.add(width);
            heights.add(height);

            int fpSize = input.readInt();
            FloatPointer fp = new FloatPointer(fpSize);

            for(int j = 0; j < fpSize; j++)
                fp.put(j, input.readFloat());

            descriptors.add(fp);
        }
    }
}