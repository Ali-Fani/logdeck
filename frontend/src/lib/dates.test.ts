import { describe, expect, it } from "vitest";

import {
	formatDate,
	formatDateTime,
	formatMonthDay,
	formatMonthDayTime,
	formatRowTimestamp,
	formatTime,
} from "./dates";

// Oct 6 2025 14:30:45 local time, which is 1404/07/14 14:30:45 in the Jalali
// calendar. Constructed from local components so the assertions hold in any
// timezone.
const INSTANT = new Date(2025, 9, 6, 14, 30, 45);

describe("formatDateTime", () => {
	it("formats Jalali dates with transliterated months and Latin digits", () => {
		expect(formatDateTime(INSTANT, "jalali")).toBe("Meh 14, 1404 14:30");
	});

	it("accepts numeric timestamps", () => {
		expect(formatDateTime(INSTANT.getTime(), "jalali")).toBe(
			"Meh 14, 1404 14:30",
		);
	});

	it("keeps the Gregorian medium date plus short time", () => {
		expect(formatDateTime(INSTANT, "gregorian")).toBe(
			INSTANT.toLocaleString(undefined, {
				dateStyle: "medium",
				timeStyle: "short",
			}),
		);
	});
});

describe("formatDate", () => {
	it("formats Jalali dates without the time", () => {
		expect(formatDate(INSTANT, "jalali")).toBe("Meh 14, 1404");
	});

	it("keeps the Gregorian default date", () => {
		expect(formatDate(INSTANT, "gregorian")).toBe(INSTANT.toLocaleDateString());
	});
});

describe("formatMonthDay", () => {
	it("formats compact Jalali month and day", () => {
		expect(formatMonthDay(INSTANT, "jalali")).toBe("Meh 14");
	});

	it("keeps the Gregorian short month and day", () => {
		expect(formatMonthDay(INSTANT, "gregorian")).toBe(
			INSTANT.toLocaleDateString(undefined, {
				month: "short",
				day: "numeric",
			}),
		);
	});
});

describe("formatMonthDayTime", () => {
	it("formats Jalali month, day and time", () => {
		expect(formatMonthDayTime(INSTANT, "jalali")).toBe("Meh 14, 14:30");
	});

	it("keeps the Gregorian month, day and time", () => {
		expect(formatMonthDayTime(INSTANT, "gregorian")).toBe("Oct 6, 14:30");
	});
});

describe("formatTime", () => {
	it("is calendar-independent", () => {
		expect(formatTime(INSTANT)).toBe("14:30");
	});
});

describe("formatRowTimestamp", () => {
	it("formats compact Jalali timestamps", () => {
		expect(formatRowTimestamp(INSTANT, "jalali")).toBe("1404/07/14 14:30:45");
	});

	it("keeps the Gregorian compact timestamp", () => {
		expect(formatRowTimestamp(INSTANT, "gregorian")).toBe(
			"06/10/2025 14:30:45",
		);
	});
});
