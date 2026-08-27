import { format as formatGregorian } from "date-fns";
import { format as formatJalali } from "date-fns-jalali";
import { enUS as jalaliEnUS } from "date-fns-jalali/locale";

import type { CalendarPreference } from "@/contexts/calendar-context";

type DateLike = Date | number;

function toDate(value: DateLike): Date {
	return value instanceof Date ? value : new Date(value);
}

/**
 * Medium date with a short time, e.g. "Oct 14, 2025, 2:30 PM" in the browser
 * locale, or "Meh 14, 1404 14:30" in Jalali. The Jalali form keeps the same
 * transliterated month names and Latin digits as the Gregorian one so both fit
 * the English UI.
 */
export function formatDateTime(
	value: DateLike,
	calendar: CalendarPreference,
): string {
	if (calendar === "jalali") {
		return formatJalali(value, "MMM d, yyyy HH:mm", {
			locale: jalaliEnUS,
		});
	}
	return toDate(value).toLocaleString(undefined, {
		dateStyle: "medium",
		timeStyle: "short",
	});
}

/**
 * Date only, e.g. "10/14/2025" in the browser locale, or "Meh 14, 1404" in
 * Jalali.
 */
export function formatDate(
	value: DateLike,
	calendar: CalendarPreference,
): string {
	if (calendar === "jalali") {
		return formatJalali(value, "MMM d, yyyy", { locale: jalaliEnUS });
	}
	return toDate(value).toLocaleDateString();
}

/**
 * Month and day only, e.g. "Oct 14" or "Meh 14". Used for compact range
 * labels.
 */
export function formatMonthDay(
	value: DateLike,
	calendar: CalendarPreference,
): string {
	if (calendar === "jalali") {
		return formatJalali(value, "MMM d", { locale: jalaliEnUS });
	}
	return toDate(value).toLocaleDateString(undefined, {
		month: "short",
		day: "numeric",
	});
}

/**
 * Month, day and time, e.g. "Oct 14, 14:30" or "Meh 14, 14:30". Used for the
 * log viewer's custom time range label.
 */
export function formatMonthDayTime(
	value: DateLike,
	calendar: CalendarPreference,
): string {
	if (calendar === "jalali") {
		return formatJalali(value, "MMM d, HH:mm", { locale: jalaliEnUS });
	}
	return formatGregorian(value, "MMM d, HH:mm");
}

/**
 * Time of day only. Wall-clock time is calendar-independent, so the output is
 * identical in both calendars.
 */
export function formatTime(value: DateLike): string {
	return formatGregorian(value, "HH:mm");
}

/**
 * Compact timestamp for dense log rows, e.g. "06/10/2025 14:30:45" or, in
 * Jalali, "1404/07/14 14:30:45".
 */
export function formatRowTimestamp(
	value: DateLike,
	calendar: CalendarPreference,
): string {
	if (calendar === "jalali") {
		return formatJalali(value, "yyyy/MM/dd HH:mm:ss", {
			locale: jalaliEnUS,
		});
	}
	const date = toDate(value);
	return `${date.toLocaleDateString("en-GB", {
		day: "2-digit",
		month: "2-digit",
		year: "numeric",
	})} ${date.toLocaleTimeString("en-US", {
		hour12: false,
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
	})}`;
}
