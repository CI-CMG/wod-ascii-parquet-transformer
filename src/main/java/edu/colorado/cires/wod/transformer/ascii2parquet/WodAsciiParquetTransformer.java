package edu.colorado.cires.wod.transformer.ascii2parquet;

import edu.colorado.cires.wod.parquet.model.Cast;
import java.util.stream.Collectors;

public final class WodAsciiParquetTransformer {

  public static Cast parquetFromAscii(edu.colorado.cires.wod.ascii.model.Cast asciiCast) {
    return Cast.builder()
        .withDataset(asciiCast.getDataset())
        .withCastNumber(asciiCast.getCastNumber())
        .withCruiseNumber(asciiCast.getCruiseNumber())
        .withOriginatorsStationCode(asciiCast.getOriginatorsStationCode())
        .withYear(asciiCast.getYear())
        .withMonth(asciiCast.getMonth())
        .withDay(asciiCast.getDay())
        .withTime(asciiCast.getTime())
        .withTimestamp(AsciiToParquet.getTimestamp(asciiCast))
        .withLongitude(asciiCast.getLongitude())
        .withLatitude(asciiCast.getLatitude())
        .withProfileType(asciiCast.getProfileType())
        .withOriginatorsStationCode(asciiCast.getOriginatorsStationCode())
        .withVariables(
            asciiCast.getVariables().stream().map(AsciiToParquet::mapVariable).collect(Collectors.toList()))
        .withPrincipalInvestigators(
            asciiCast.getPrincipalInvestigators().stream().map(AsciiToParquet::mapPi).collect(Collectors.toList()))
        .withAttributes(
            asciiCast.getAttributes().stream().map(AsciiToParquet::mapAttribute).collect(Collectors.toList()))
        .withBiologicalAttributes(
            asciiCast.getBiologicalAttributes().stream().map(AsciiToParquet::mapAttribute).collect(Collectors.toList()))
        .withTaxonomicDatasets(
            asciiCast.getTaxonomicDatasets().stream().map(AsciiToParquet::mapTaxonomicDataset).collect(Collectors.toList()))
        .withDepths(
            asciiCast.getDepths().stream().map(AsciiToParquet::mapDepth).collect(Collectors.toList()))
        .build();
  }


  private WodAsciiParquetTransformer() {

  }

}
