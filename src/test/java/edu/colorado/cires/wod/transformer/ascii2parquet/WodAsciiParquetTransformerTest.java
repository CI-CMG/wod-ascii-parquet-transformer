package edu.colorado.cires.wod.transformer.ascii2parquet;


import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.colorado.cires.wod.ascii.WodFileReader.CharReader;
import edu.colorado.cires.wod.ascii.reader.BufferedCharReader;
import edu.colorado.cires.wod.ascii.reader.CastFileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

class WodAsciiParquetTransformerTest {

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

}