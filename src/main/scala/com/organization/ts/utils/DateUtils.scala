package com.organization.ts.utils

import java.time.{DayOfWeek, Instant, ZoneId, ZonedDateTime}

import org.joda.time.DateTime
import java.text.SimpleDateFormat


object DateUtils {

  def extractHrFromEpoch(inputDate: Long): Int = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(inputDate), ZoneId.of("Etc/UTC")).getHour
  }

  def extractDayFromEpoch(inputDate: Long): Int = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(inputDate), ZoneId.of("Etc/UTC")).getDayOfMonth
  }

  def extractMonthFromEpoch(inputDate: Long): Int = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(inputDate), ZoneId.of("Etc/UTC")).getMonthValue

  }
  def extractYearFromEpoch(inputDate: Long): Int = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(inputDate), ZoneId.of("Etc/UTC")).getYear
  }

  def dateFromEpoch(inputDate: Long): String = {
    new DateTime(inputDate).toString("yyyy-MM-dd")
  }

  def epochToDate(epochMillis: Long): String = {
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(epochMillis)
  }

}
