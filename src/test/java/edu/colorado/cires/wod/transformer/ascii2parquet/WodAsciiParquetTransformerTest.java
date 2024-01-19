package edu.colorado.cires.wod.transformer.ascii2parquet;


import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.ascii.WodFileReader.CharReader;
import edu.colorado.cires.wod.ascii.reader.BufferedCharReader;
import edu.colorado.cires.wod.ascii.reader.CastFileReader;
import edu.colorado.cires.wod.parquet.model.Cast;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class WodAsciiParquetTransformerTest {

  private static final int GEOHASH_LENGTH = 3;

  @Test
  public void testParquetFromAscii() throws Exception {
    List<edu.colorado.cires.wod.parquet.model.Cast> parquetCasts = new ArrayList<>();
    try (BufferedReader bufferedReader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(Files.newInputStream(Paths.get("src/test/resources/wod/APB/OBS/APBO1997.gz")))))) {
      // Could also have used a RandomAccessFileCharReader.  BufferedCharReader is more flexible as it can read from any input stream.
      CharReader characterReader = new BufferedCharReader(bufferedReader);
      CastFileReader reader = new CastFileReader(characterReader, "APB");
      while (reader.hasNext()) {
        edu.colorado.cires.wod.parquet.model.Cast parquetCast = WodAsciiParquetTransformer.parquetFromAscii(reader.next(), GEOHASH_LENGTH);
        parquetCasts.add(parquetCast);
      }
    }
    assertEquals(1, parquetCasts.size());
//    System.out.println(parquetCasts.get(0));
    //TODO assert on converted cast
  }

  @Test
  public void testAsciiFromParquet() {
    // TODO
  }

  @Test
  public void testSpark() throws Exception {
    List<edu.colorado.cires.wod.parquet.model.Cast> parquetCasts = new ArrayList<>();
    try (BufferedReader bufferedReader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(Files.newInputStream(Paths.get("src/test/resources/wod/APB/OBS/APBO1997.gz")))))) {
      // Could also have used a RandomAccessFileCharReader.  BufferedCharReader is more flexible as it can read from any input stream.
      CharReader characterReader = new BufferedCharReader(bufferedReader);
      CastFileReader reader = new CastFileReader(characterReader, "APB");
      while (reader.hasNext()) {
        edu.colorado.cires.wod.parquet.model.Cast parquetCast = WodAsciiParquetTransformer.parquetFromAscii(reader.next(), GEOHASH_LENGTH);
        parquetCasts.add(parquetCast);
      }
    }

    System.out.println(parquetCasts.get(0));

    try(SparkSession spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()) {
      Dataset<Cast> dataset = spark.createDataset(parquetCasts, Encoders.bean(Cast.class));
      dataset.printSchema();
      dataset.write().parquet("target/test.parquet");
      System.out.println("Saved Dataset");

      Dataset<Row> ds = spark.read().parquet("target/test.parquet");

      System.out.println("Read Dataset");
      System.out.println("Parquet Schema");
      ds.printSchema();
      System.out.println("Parquet Data");
      ds.show();
    }
  }

}