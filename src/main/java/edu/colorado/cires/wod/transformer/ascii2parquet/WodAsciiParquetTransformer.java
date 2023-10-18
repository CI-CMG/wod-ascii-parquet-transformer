package edu.colorado.cires.wod.transformer.ascii2parquet;

import com.github.davidmoten.geo.GeoHash;
import edu.colorado.cires.wod.parquet.model.Attribute;
import edu.colorado.cires.wod.parquet.model.Cast;
import edu.colorado.cires.wod.parquet.model.Depth;
import edu.colorado.cires.wod.parquet.model.Metadata;
import edu.colorado.cires.wod.parquet.model.PrincipalInvestigator;
import edu.colorado.cires.wod.parquet.model.ProfileData;
import edu.colorado.cires.wod.parquet.model.QcAttribute;
import edu.colorado.cires.wod.parquet.model.TaxonomicDataset;
import edu.colorado.cires.wod.parquet.model.Variable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

public class WodAsciiParquetTransformer {

  public static Cast parquetFromAscii(edu.colorado.cires.wod.ascii.model.Cast asciiCast, int geohashLength) {
    return Cast.builder()
        .withCastNumber(asciiCast.getCastNumber())
        .withCruiseNumber(asciiCast.getCruiseNumber())
        .withOriginatorsStationCode(asciiCast.getOriginatorsStationCode())
        .withYear(asciiCast.getYear())
        .withMonth(asciiCast.getMonth())
        .withDay(asciiCast.getDay())
        .withTime(asciiCast.getTime() == null ? 0D : asciiCast.getTime())
        .withTimestamp(getTimestamp(asciiCast))
        .withLongitude(asciiCast.getLongitude())
        .withLatitude(asciiCast.getLatitude())
        .withProfileType(asciiCast.getProfileType())
        .withOriginatorsStationCode(asciiCast.getOriginatorsStationCode())
        .withGeohash(GeoHash.encodeHash(asciiCast.getLatitude(), asciiCast.getLongitude(), geohashLength))
        .withVariables(asciiCast.getVariables().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .withPrincipalInvestigators(
            asciiCast.getPrincipalInvestigators().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .withAttributes(asciiCast.getAttributes().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .withBiologicalAttributes(asciiCast.getBiologicalAttributes().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .withTaxonomicDatasets(asciiCast.getTaxonomicDatasets().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .withDepths(asciiCast.getDepths().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .build();
  }

  public static edu.colorado.cires.wod.ascii.model.Cast asciiFromParquet(Cast parquetCast) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private static long getTimestamp(edu.colorado.cires.wod.ascii.model.Cast cast) {
    HourMin hourMin = getTime(cast);
    return LocalDateTime.of(
        cast.getYear(),
        cast.getMonth(),
        cast.getDay() == null ? 1 : cast.getDay(),
        hourMin.getHour(),
        hourMin.getMin()
    ).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  }

  private static HourMin getTime(edu.colorado.cires.wod.ascii.model.Cast cast) {
    Double time = cast.getTime();
    if (time == null) {
      return new HourMin(0, 0);
    }

    double hoursWithFractionalHours = cast.getTime();
    int wholeHours = (int) hoursWithFractionalHours;
    if (wholeHours == 24) {
      return new HourMin(0, 0);
    }
    double fractionalHours = hoursWithFractionalHours - (double) wholeHours;
    int minutes = (int) (60D * fractionalHours);

    return new HourMin(wholeHours, minutes);

  }

  private static Variable map(edu.colorado.cires.wod.ascii.Variable asciiVariable) {
    return Variable.builder()
        .withCode(asciiVariable.getCode())
        .withMetadata(asciiVariable.getMetadata().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .build();
  }

  private static Metadata map(edu.colorado.cires.wod.ascii.Metadata asciiMd) {
    return Metadata.builder().withCode(asciiMd.getCode()).withValue(asciiMd.getValue()).build();
  }

  private static PrincipalInvestigator map(edu.colorado.cires.wod.ascii.model.PrincipalInvestigator asciiPi) {
    return PrincipalInvestigator.builder().withCode(asciiPi.getCode()).withVariable(asciiPi.getVariable()).build();
  }

  private static Attribute map(edu.colorado.cires.wod.ascii.model.Attribute asciiAttribute) {
    return Attribute.builder().withCode(asciiAttribute.getCode()).withValue(asciiAttribute.getValue()).build();
  }

  private static TaxonomicDataset map(List<edu.colorado.cires.wod.ascii.model.QcAttribute> asciiQcAttributes) {
    return TaxonomicDataset.builder().withAttributes(asciiQcAttributes.stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList())).build();
  }

  private static QcAttribute map(edu.colorado.cires.wod.ascii.model.QcAttribute asciiQcAttribute) {
    return QcAttribute.builder()
        .withCode(asciiQcAttribute.getCode())
        .withValue(asciiQcAttribute.getValue())
        .withQcFlag(asciiQcAttribute.getQcFlag())
        .withOriginatorsFlag(asciiQcAttribute.getOriginatorsFlag())
        .build();
  }

  private static Depth map(edu.colorado.cires.wod.ascii.model.Depth asciiDepth) {
    return Depth.builder()
        .withDepth(asciiDepth.getDepth())
        .withOriginatorsFlag(asciiDepth.getOriginatorsFlag())
        .withDepthErrorFlag(asciiDepth.getDepthErrorFlag())
        .withData(asciiDepth.getData().stream().map(WodAsciiParquetTransformer::map).collect(Collectors.toList()))
        .build();
  }

  private static ProfileData map(edu.colorado.cires.wod.ascii.model.ProfileData asciiData) {
    return ProfileData.builder()
        .withVariable(asciiData.getVariable())
        .withValue(asciiData.getValue())
        .withQcFlag(asciiData.getQcFlag())
        .withOriginatorsFlag(asciiData.getOriginatorsFlag())
        .build();
  }

}