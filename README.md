#VLDTopology

##Installtion (for coding/compiling)
You will need a few software:

1. Storm, http://storm.apache.org/
2. Zookeeper, https://zookeeper.apache.org/
3. OpenCV, http://opencv.org/ (and OPENCV_DIR to the binaries)
4. Redis, http://redis.io/
5. IntelliJ, https://www.jetbrains.com/idea/
6. Maven, if it doesn't come with IntelliJ.
7. Node.js for the Settings and Logo Manager (see https://github.com/blackhydrogen/SettingsLogoManager for more instructions.)

##Running
To run, you will first need to start the supporting applications, and in this order:

1. Zookeeper
2. Storm's Nimbus
3. Storm's Supervisor
4. Storm's (web) UI
5. Settings and Logo Manager (SLM)

There is a batch script in `/misc/stormservice.bat` that automates this, but you will have to update the paths accordingly.

Note that Redis should be running as a background service (or on a network).

Next, you run 3 Storm applications:

1. The main application that does the processing.
2. The input application that takes a video/folder-of-frames and feed it into the main application.
3. The output application that takes the processed frames and converts it back into a video/folder-of-processed-frames.

The batch file `/misc/runproj.bat` opens 3 command prompt windows and suggests (via echo) the input format to run the 3 Storm applications.

Finally open localhost:3000 for the Settings and Logo Manager. (Server should be running after running `/misc/stormservice.bat`.)

##Upgrading from JavaCV 0.8 to 0.9
An upgrade of the JavaCV 0.8 to 0.9 was done for this fork. It isn't as simple as changing the respective pom.xml; it requires some manual inclusions. Steps below:

1. Update the pom.xml from JavaCV 0.8 to 0.9. Let Maven update the dependencies. It will compile but will not run (unlinked binary files).
2. To solve the unlinked binary files issues, you need to manually download the JavaCV 0.9 binary files that Maven uses. You can do this at http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.bytedeco%22%20AND%20a%3A%22javacv%22 . Make sure you download version 0.9. (`javacv-0.9-bin.zip`)
3. Open the ZIP file (`javacv-0.9-bin.zip`).
4. Look for the JAR files (inside the JAR file above) named `opencv-OS_NAME-ARCHITECTURE.jar`. E.g. `opencv-windows-x86_64.jar`.
5. Pick the JAR files that matches the OS/architectures you intent to run the application on.
6. Extract all the files in the `opencv-OS_NAME-ARCHITECTURE.jar` files into the `/src/main/resources/` folder. Exclude the META-INF folder. You should have files inside the `/src/main/resources/org/bytedeco/...` folders.
7. Re-compile. Maven will include all files in the `/src/main/resources/` folder automatically.
8. This should resolve the binary dependency issue as it is now included in the final package/JAR file (you compiled).

Note that the `/src/main/resources/` folder is excluded from this repository (i.e. it's in `.gitignore`.
