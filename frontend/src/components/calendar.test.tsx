import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { Calendar } from "@/components/ui/calendar";
import { CalendarProvider } from "@/contexts/calendar-context";

// Oct 6 2025 is 1404/07/14 in the Jalali calendar, so this fixed month makes
// both calendars show a deterministic caption.
const FIXED_MONTH = new Date(2025, 9, 6);

function renderCalendar(preference: "gregorian" | "jalali") {
	localStorage.setItem("logdeck_calendar", preference);
	return render(
		<CalendarProvider>
			<Calendar mode="single" month={FIXED_MONTH} />
		</CalendarProvider>,
	);
}

afterEach(() => {
	localStorage.removeItem("logdeck_calendar");
});

describe("Calendar", () => {
	it("shows Gregorian months by default", () => {
		renderCalendar("gregorian");
		expect(screen.getByText("October 2025")).toBeTruthy();
	});

	it("shows Jalali months when the calendar preference is jalali", () => {
		renderCalendar("jalali");
		expect(screen.getByText("Mehr 1404")).toBeTruthy();
	});
});
