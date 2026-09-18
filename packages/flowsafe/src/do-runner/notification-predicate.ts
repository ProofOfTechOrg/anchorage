// SPDX-License-Identifier: Apache-2.0

const NOTIFICATION_TIMESTAMP_COLUMNS = [
  'deliverAt',
  'summaryAt',
  'updatedAt',
] as const;

type NotificationTimestampColumn =
  (typeof NOTIFICATION_TIMESTAMP_COLUMNS)[number];

const ISO_YEAR = '(?:\\d{4}|[+-]\\d{6})';
const ISO_MONTH = '(?:0[1-9]|1[0-2])';
const ISO_DAY = '(?:0[1-9]|[12]\\d|3[01])';
const ISO_HOUR = '(?:[01]\\d|2[0-4])';
const ISO_MINUTE = '[0-5]\\d';
const ISO_ZONE = '(?:Z|[+-](?:[01]\\d|2[0-3]):?[0-5]\\d)';
const ISO_DATE = `${ISO_YEAR}-${ISO_MONTH}-${ISO_DAY}`;
const ISO_DATE_ONLY = `${ISO_YEAR}(?:-${ISO_MONTH}(?:-${ISO_DAY})?)?`;
const ISO_TIME = `${ISO_HOUR}:${ISO_MINUTE}(?::${ISO_MINUTE}(?:\\.\\d+)?)?`;
const NOTIFICATION_TIMESTAMP = new RegExp(
  `^(?:${ISO_DATE_ONLY}|${ISO_DATE}T${ISO_TIME}${ISO_ZONE})(?![\\s\\S])`,
  'i',
);

export function notificationTimestampMillis(value: Date | string): number {
  const time =
    value instanceof Date
      ? value.getTime()
      : typeof value === 'string' &&
          NOTIFICATION_TIMESTAMP.test(value) &&
          !value.startsWith('-000000')
        ? new Date(value).getTime()
        : NaN;
  if (!Number.isFinite(time)) {
    throw new TypeError(
      'Notification timestamp must be a finite Date or supported ISO date',
    );
  }
  return time;
}

// Whole Gregorian cycles preserve month/day relationships while SQLite parses
// a year in its supported range; integer milliseconds retain the original range.
// Materializing per-value stages bounds SQLite's compiled expression growth.
export function notificationTimestampSql(
  column: NotificationTimestampColumn,
): string {
  if (!NOTIFICATION_TIMESTAMP_COLUMNS.includes(column)) {
    throw new TypeError('Unknown notification timestamp column');
  }
  return `(CASE WHEN typeof(${column}) = 'text'
    AND length(CAST(${column} AS BLOB)) = 24
    AND ${column} GLOB '????-??-??T??:??:??.???Z'
    AND CAST(substr(${column}, 12, 2) AS INTEGER) < 24
    THEN CAST(ROUND((julianday(${column}) - 2440587.5) * 86400000) AS INTEGER)
    ELSE (WITH iso_source AS (
    SELECT ${column} AS raw
  ), iso_input AS (
    SELECT raw, upper(raw) AS value,
      CASE WHEN substr(raw, 1, 1) IN ('+', '-') THEN 7 ELSE 4 END AS year_width
    FROM iso_source
  ), iso_split AS (
    SELECT *, instr(value, 'T') AS time_start FROM iso_input
  ), iso_parts AS MATERIALIZED (
    SELECT *,
      CASE WHEN time_start = 0 THEN value ELSE substr(value, 1, time_start - 1) END AS date_part,
      CASE WHEN time_start = 0 THEN NULL ELSE substr(value, time_start + 1) END AS zoned_time
    FROM iso_split
  ), iso_zone AS (
    SELECT *,
      CASE
        WHEN zoned_time IS NULL THEN 'date'
        WHEN substr(zoned_time, -1) = 'Z' THEN 'utc'
        WHEN substr(zoned_time, -6, 1) IN ('+', '-') THEN 'offset'
        WHEN substr(zoned_time, -5, 1) IN ('+', '-') THEN 'compact'
        ELSE 'missing'
      END AS zone_type
    FROM iso_parts
  ), iso_clock AS MATERIALIZED (
    SELECT *,
      CAST(substr(date_part, 1, year_width) AS INTEGER) AS original_year,
      CASE WHEN year_width = 7 THEN substr(date_part, 2, 6) ELSE substr(date_part, 1, 4) END AS year_digits,
      CASE WHEN length(date_part) >= year_width + 3 THEN substr(date_part, year_width + 2, 2) ELSE '01' END AS month,
      CASE WHEN length(date_part) >= year_width + 6 THEN substr(date_part, year_width + 5, 2) ELSE '01' END AS day,
      CASE zone_type
        WHEN 'date' THEN '00:00:00'
        WHEN 'utc' THEN substr(zoned_time, 1, length(zoned_time) - 1)
        WHEN 'offset' THEN substr(zoned_time, 1, length(zoned_time) - 6)
        WHEN 'compact' THEN substr(zoned_time, 1, length(zoned_time) - 5)
        ELSE ''
      END AS clock,
      CASE zone_type
        WHEN 'offset' THEN substr(zoned_time, -6)
        WHEN 'compact' THEN substr(zoned_time, -5, 3) || ':' || substr(zoned_time, -2)
        ELSE '+00:00'
      END AS zone
    FROM iso_zone
  ), iso_fields AS MATERIALIZED (
    SELECT *,
      substr(clock, 1, 2) AS hour,
      substr(clock, 4, 2) AS minute,
      CASE WHEN length(clock) >= 8 THEN substr(clock, 7, 2) ELSE '00' END AS second,
      CASE WHEN length(clock) > 8 THEN substr(clock, 10) ELSE '' END AS fraction,
      substr(zone, 2, 2) AS zone_hour,
      substr(zone, 5, 2) AS zone_minute,
      2000 + ((original_year % 400 + 400) % 400) AS mapped_year
    FROM iso_clock
  ), iso_valid AS (
    SELECT *, substr(fraction || '000', 1, 3) AS milliseconds
    FROM iso_fields
    WHERE typeof(raw) = 'text'
      AND length(CAST(raw AS BLOB)) = length(raw)
      AND length(year_digits) = CASE WHEN year_width = 7 THEN 6 ELSE 4 END
      AND year_digits NOT GLOB '*[^0-9]*'
      AND substr(date_part, 1, 7) != '-000000'
      AND length(date_part) IN (year_width, year_width + 3, year_width + 6)
      AND (length(date_part) = year_width OR substr(date_part, year_width + 1, 1) = '-')
      AND (length(date_part) < year_width + 6 OR substr(date_part, year_width + 4, 1) = '-')
      AND (time_start = 0 OR length(date_part) = year_width + 6)
      AND length(month) = 2 AND month NOT GLOB '*[^0-9]*' AND CAST(month AS INTEGER) BETWEEN 1 AND 12
      AND length(day) = 2 AND day NOT GLOB '*[^0-9]*' AND CAST(day AS INTEGER) BETWEEN 1 AND 31
      AND zone_type != 'missing'
      AND (length(clock) IN (5, 8) OR (length(clock) >= 10 AND substr(clock, 9, 1) = '.'))
      AND substr(clock, 3, 1) = ':'
      AND (length(clock) = 5 OR substr(clock, 6, 1) = ':')
      AND length(hour) = 2 AND hour NOT GLOB '*[^0-9]*' AND CAST(hour AS INTEGER) BETWEEN 0 AND 24
      AND length(minute) = 2 AND minute NOT GLOB '*[^0-9]*' AND CAST(minute AS INTEGER) BETWEEN 0 AND 59
      AND length(second) = 2 AND second NOT GLOB '*[^0-9]*' AND CAST(second AS INTEGER) BETWEEN 0 AND 59
      AND fraction NOT GLOB '*[^0-9]*'
      AND (CAST(hour AS INTEGER) < 24 OR (CAST(minute AS INTEGER) = 0 AND CAST(second AS INTEGER) = 0 AND CAST(substr(fraction || '000', 1, 3) AS INTEGER) = 0))
      AND substr(zone, 4, 1) = ':'
      AND length(zone_hour) = 2 AND zone_hour NOT GLOB '*[^0-9]*' AND CAST(zone_hour AS INTEGER) BETWEEN 0 AND 23
      AND length(zone_minute) = 2 AND zone_minute NOT GLOB '*[^0-9]*' AND CAST(zone_minute AS INTEGER) BETWEEN 0 AND 59
  ), iso_epoch AS (
    SELECT
      CAST(ROUND((julianday(printf('%04d-%02d-%02dT%02d:%02d:%02d.%sZ',
        mapped_year, CAST(month AS INTEGER), CAST(day AS INTEGER),
        CAST(hour AS INTEGER), CAST(minute AS INTEGER), CAST(second AS INTEGER), milliseconds))
        - 2440587.5) * 86400000) AS INTEGER)
      - (CASE WHEN substr(zone, 1, 1) = '-' THEN -1 ELSE 1 END)
        * (CAST(zone_hour AS INTEGER) * 60 + CAST(zone_minute AS INTEGER)) * 60000
      + ((original_year - mapped_year) / 400) * 146097 * 86400000 AS epoch_ms
    FROM iso_valid
  )
  SELECT CASE WHEN epoch_ms BETWEEN -8640000000000000 AND 8640000000000000 THEN epoch_ms END
  FROM iso_epoch) END)`;
}

/** Bind numeric epoch milliseconds for deliverAt, then summaryAt. */
export const DUE_NOTIFICATION_SQL = `status = 'pending' AND ((${notificationTimestampSql('deliverAt')}) <= ? OR (${notificationTimestampSql('summaryAt')}) <= ?)`;
