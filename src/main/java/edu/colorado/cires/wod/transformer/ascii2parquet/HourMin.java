package edu.colorado.cires.wod.transformer.ascii2parquet;

class HourMin {

  private final int hour;
  private final int min;


  HourMin(int hour, int min) {
    this.hour = hour;
    this.min = min;
  }

  int getHour() {
    return hour;
  }

  int getMin() {
    return min;
  }
}
