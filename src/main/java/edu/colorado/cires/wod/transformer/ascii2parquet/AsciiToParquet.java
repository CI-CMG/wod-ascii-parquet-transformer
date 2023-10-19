package edu.colorado.cires.wod.transformer.ascii2parquet;

import edu.colorado.cires.wod.parquet.model.Attribute;
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

final class AsciiToParquet {

  static HourMin getTime(edu.colorado.cires.wod.ascii.model.Cast cast) {
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

  static long getTimestamp(edu.colorado.cires.wod.ascii.model.Cast cast) {
    HourMin hourMin = getTime(cast);
    return LocalDateTime.of(
        cast.getYear(),
        cast.getMonth(),
        cast.getDay() == null ? 1 : cast.getDay(),
        hourMin.getHour(),
        hourMin.getMin()
    ).atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
  }

  static Metadata mapMetadata(edu.colorado.cires.wod.ascii.Metadata asciiMd) {
    return Metadata.builder().withCode(asciiMd.getCode()).withValue(asciiMd.getValue()).build();
  }

  static Variable mapVariable(edu.colorado.cires.wod.ascii.Variable asciiVariable) {
    return Variable.builder()
        .withCode(asciiVariable.getCode())
        .withMetadata(asciiVariable.getMetadata().stream().map(AsciiToParquet::mapMetadata).collect(Collectors.toList()))
        .build();
  }

  static PrincipalInvestigator mapPi(edu.colorado.cires.wod.ascii.model.PrincipalInvestigator asciiPi) {
    return PrincipalInvestigator.builder().withCode(asciiPi.getCode()).withVariable(asciiPi.getVariable()).build();
  }

  static Attribute mapAttribute(edu.colorado.cires.wod.ascii.model.Attribute asciiAttribute) {
    return Attribute.builder().withCode(asciiAttribute.getCode()).withValue(asciiAttribute.getValue()).build();
  }

  static QcAttribute mapQcAttribute(edu.colorado.cires.wod.ascii.model.QcAttribute asciiQcAttribute) {
    return QcAttribute.builder()
        .withCode(asciiQcAttribute.getCode())
        .withValue(asciiQcAttribute.getValue())
        .withQcFlag(asciiQcAttribute.getQcFlag())
        .withOriginatorsFlag(asciiQcAttribute.getOriginatorsFlag())
        .build();
  }

  static TaxonomicDataset mapTaxonomicDataset(List<edu.colorado.cires.wod.ascii.model.QcAttribute> asciiQcAttributes) {
    return TaxonomicDataset.builder()
        .withAttributes(asciiQcAttributes.stream().map(AsciiToParquet::mapQcAttribute).collect(Collectors.toList()))
        .build();
  }

  static ProfileData mapProfileData(edu.colorado.cires.wod.ascii.model.ProfileData asciiData) {
    return ProfileData.builder()
        .withVariable(asciiData.getVariable())
        .withValue(asciiData.getValue())
        .withQcFlag(asciiData.getQcFlag())
        .withOriginatorsFlag(asciiData.getOriginatorsFlag())
        .build();
  }

  static Depth mapDepth(edu.colorado.cires.wod.ascii.model.Depth asciiDepth) {
    return Depth.builder()
        .withDepth(asciiDepth.getDepth())
        .withOriginatorsFlag(asciiDepth.getOriginatorsFlag())
        .withDepthErrorFlag(asciiDepth.getDepthErrorFlag())
        .withData(asciiDepth.getData().stream().map(AsciiToParquet::mapProfileData).collect(Collectors.toList()))
        .build();
  }

  private AsciiToParquet() {

  }
}
