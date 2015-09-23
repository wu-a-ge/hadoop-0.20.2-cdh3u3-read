package org.apache.hadoop.tools;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.SplitOutputStream;
import net.lingala.zip4j.io.ZipInputStream;
import net.lingala.zip4j.io.ZipOutputStream;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipModel;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tools.tar.TarBuffer;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.apache.tools.tar.TarOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Use for Tar and Zip(with split size) directory from any Hadoop fs to local
 * 
 * @author xiaohan cqvip
 * 
 */
public class Tar {

  static class TarHelper {

    /**
     * Steal the method from net.lingala.zip4j.zip.ZipEngine, refer to its
     * addStreamToZip()
     * 
     * @param fileName
     * @param splitSize
     * @return
     * @throws Exception
     */
    public static ZipOutputStream createZipOutputStream(String baseName,
        long splitSize) throws Exception {
      ZipFile zipFile = new ZipFile(
          new File(baseName + ".zip").getAbsoluteFile());

      ZipParameters parameters = new ZipParameters();
      parameters.setCompressionMethod(Zip4jConstants.COMP_STORE);
      parameters.setSourceExternalStream(true);
      parameters.setFileNameInZip(new File(baseName).getName() + ".tar");

      // use reflect to create the ZipModel
      Method method = ZipFile.class.getDeclaredMethod("createNewZipModel");
      method.setAccessible(true);
      method.invoke(zipFile);

      // use reflect to set split parameter in the ZipModel
      Field field = ZipFile.class.getDeclaredField("zipModel");
      field.setAccessible(true);
      ZipModel zipModel = (ZipModel) field.get(zipFile);
      zipModel.setSplitLength(splitSize);
      zipModel.setSplitArchive(true);

      // create zipout underlined with splitout
      SplitOutputStream splitOutputStream = new SplitOutputStream(new File(
          zipModel.getZipFile()), zipModel.getSplitLength());
      ZipOutputStream zipOut = new ZipOutputStream(splitOutputStream, zipModel);
      zipOut.putNextEntry(null, parameters);

      return zipOut;
    }

    public static void endZipOutputStream(ZipOutputStream zipOut)
        throws IOException, ZipException {
      zipOut.closeEntry();
      zipOut.finish();
    }

    /**
     * TarOutputStream buffer the data, a little hard to flush it to underline.
     * After call it, should not call close() again
     * 
     * @param tarOut
     * @throws Exception
     */
    public static void flushTarOutputStream(TarOutputStream tarOut)
        throws Exception {
      // finish now
      tarOut.finish();

      // get the internal buffer
      Field field = tarOut.getClass().getDeclaredField("buffer");
      field.setAccessible(true);
      TarBuffer tarBuffer = (TarBuffer) field.get(tarOut);

      // flush the buffer
      Method method = tarBuffer.getClass().getDeclaredMethod("flushBlock");
      method.setAccessible(true);
      method.invoke(tarBuffer);
    }

  }

  private static final long MB = 1024 * 1024;
  private static final long GB = 1024 * MB;

  private static FileSystem fs = null;

  private static final String REVERSE = "reverse";
  private static final String SKIP_SPECIAL = "skipspecial";
  private static final String SPECIAL_TOKEN = "_";

  private static Configuration conf = new Configuration();
  private static Logger LOG = LoggerFactory.getLogger(Tar.class);

  private static Options options = new Options();
  static {
    options.addOption(new Option(REVERSE, false,
        "Untar the local files to hdfs directly"));
    options.addOption(new Option(SKIP_SPECIAL, false,
        "Skip internal files start with [" + SPECIAL_TOKEN + "]"));
  }

  public static void main(String[] args) throws Exception {

    // args = new String[] { "hdfs://node201.vipcloud:9000/xh", "xh",
    // "-skipspecial", "65536" };

    GnuParser parser = new GnuParser();
    CommandLine cmd = null;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      printUsage(options);
      return;
    }

    String[] leftArgs = cmd.getArgs();
    if (leftArgs.length < 2 || leftArgs.length > 3) {
      printUsage(options);
      return;
    }

    String src = leftArgs[0];
    String dest = leftArgs[1];

    if (cmd.hasOption(REVERSE)) {
      doUnTar(src, dest);
    } else {
      long splitSize = 4 * GB;
      if (leftArgs.length == 3) {
        splitSize = parseSize(leftArgs[2]);
      }
      doTar(src, dest, splitSize, cmd.hasOption(SKIP_SPECIAL));
    }
  }

  private static void printUsage(Options options) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("hadoop " + Tar.class.getName()
        + " <src> <dest> [splitsize]", options);
  }

  private static long parseSize(String size) {
    String noramlizeSize = size.toUpperCase();
    if (noramlizeSize.endsWith("GB")) {
      return Long.parseLong(size.substring(0, size.length() - 2)) * GB;
    } else if (noramlizeSize.endsWith("MB")) {
      return Long.parseLong(size.substring(0, size.length() - 2)) * MB;
    }
    return Long.parseLong(size);
  }

  private static void doTar(String src, String local, long splitSize,
      boolean skipspecial) throws Exception {
    Path srcPath = new Path(src);
    fs = srcPath.getFileSystem(conf);

    if (fs.isFile(srcPath)) {
      System.err.println("Error, only directory is supported: " + srcPath);
      return;
    }

    ZipOutputStream zipOut = TarHelper.createZipOutputStream(local, splitSize);
    TarOutputStream tarOut = null;

    // FileOutputStream zipOut = new FileOutputStream(new File(
    // "./hdfstest2.tar"));
    try {
      tarOut = new TarOutputStream(zipOut);
      tarOut.putNextEntry(new TarEntry(srcPath.getName() + Path.SEPARATOR));

      __tar(tarOut, srcPath, srcPath.getName() + Path.SEPARATOR, skipspecial);

      // first flush the tarOut
      TarHelper.flushTarOutputStream(tarOut);

      // then, end the zipEntry
      TarHelper.endZipOutputStream(zipOut);
    } finally {
      if (zipOut != null) {
        zipOut.close();
      }
    }

  }

  private static void __tar(TarOutputStream out, Path srcPath,
      String parentEntryName, boolean skipspecial) throws IOException {

    for (FileStatus status : fs.listStatus(srcPath)) {
      if (skipspecial && status.getPath().getName().startsWith(SPECIAL_TOKEN)) {
        continue;
      }
      if (status.isDir()) {
        out.putNextEntry(new TarEntry(srcPath.getName() + Path.SEPARATOR));
        Path deeperSrcPath = status.getPath();
        __tar(out, deeperSrcPath, parentEntryName + deeperSrcPath.getName()
            + Path.SEPARATOR, skipspecial);
      } else {
        Path deeperSrcPath = status.getPath();
        TarEntry tarEntry = new TarEntry(parentEntryName
            + deeperSrcPath.getName());
        tarEntry.setSize(status.getLen());
        out.putNextEntry(tarEntry);
        FSDataInputStream in = null;
        try {
          in = fs.open(deeperSrcPath);
          IOUtils.copyLarge(in, out);
        } catch (IOException e) {
          LOG.error("Failed in copying " + deeperSrcPath, e);
          throw e;
        } finally {
          try {
            if (in != null) {
              in.close();
            }
          } finally {
            out.closeEntry();
          }
        }
      }
    }
  }

  private static void doUnTar(String local, String dest) throws Exception {
    Path destPath = new Path(dest);
    fs = destPath.getFileSystem(conf);

    ZipFile zipFile = new ZipFile(local);

    @SuppressWarnings("unchecked")
    List<FileHeader> fileHeaderList = zipFile.getFileHeaders();
    FileHeader fileHeader = fileHeaderList.get(0);
    ZipInputStream is = zipFile.getInputStream(fileHeader);

    TarInputStream tarIn = null;
    try {
      tarIn = new TarInputStream(is);

      fs.mkdirs(destPath);
      TarEntry entry = null;

      while ((entry = tarIn.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          fs.mkdirs(new Path(destPath, entry.getName()));
        } else {
          OutputStream out = null;

          try {
            out = fs.create(new Path(destPath, entry.getName()));
            IOUtils.copy(tarIn, out);
          } catch (IOException e) {
            LOG.error("Failed in copying " + entry.getName(), e);
            throw e;
          } finally {
            if (out != null)
              out.close();
          }

        }
      }

    } finally {
      if (tarIn != null) {
        tarIn.close();
      }
    }
  }
}
