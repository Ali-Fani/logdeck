import type React from "react";
import { createContext, useCallback, useContext, useState } from "react";

export type CalendarPreference = "gregorian" | "jalali";

// Client-side display preference, stored like the theme (and the auth token
// before it). The server always speaks UTC instants; only the presentation
// changes with this value.
const STORAGE_KEY = "logdeck_calendar";

interface CalendarContextType {
	calendar: CalendarPreference;
	setCalendar: (value: CalendarPreference) => void;
	isJalali: boolean;
}

const CalendarContext = createContext<CalendarContextType | undefined>(
	undefined,
);

function readStoredPreference(): CalendarPreference {
	try {
		return localStorage.getItem(STORAGE_KEY) === "jalali"
			? "jalali"
			: "gregorian";
	} catch {
		return "gregorian";
	}
}

export function CalendarProvider({ children }: { children: React.ReactNode }) {
	const [calendar, setCalendarState] =
		useState<CalendarPreference>(readStoredPreference);

	const setCalendar = useCallback((value: CalendarPreference) => {
		localStorage.setItem(STORAGE_KEY, value);
		setCalendarState(value);
	}, []);

	const value: CalendarContextType = {
		calendar,
		setCalendar,
		isJalali: calendar === "jalali",
	};

	return (
		<CalendarContext.Provider value={value}>
			{children}
		</CalendarContext.Provider>
	);
}

export function useCalendar() {
	const context = useContext(CalendarContext);
	if (context === undefined) {
		throw new Error("useCalendar must be used within a CalendarProvider");
	}
	return context;
}
