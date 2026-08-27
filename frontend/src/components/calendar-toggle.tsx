import { CalendarDaysIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuRadioGroup,
	DropdownMenuRadioItem,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
	Tooltip,
	TooltipContent,
	TooltipTrigger,
} from "@/components/ui/tooltip";
import {
	type CalendarPreference,
	useCalendar,
} from "@/contexts/calendar-context";

export function CalendarToggle() {
	const { calendar, setCalendar } = useCalendar();

	return (
		<DropdownMenu>
			<Tooltip>
				<TooltipTrigger asChild>
					<DropdownMenuTrigger asChild>
						<Button variant="ghost" size="sm" className="h-9 w-9 p-0">
							<CalendarDaysIcon className="size-4" />
							<span className="sr-only">Toggle calendar</span>
						</Button>
					</DropdownMenuTrigger>
				</TooltipTrigger>
				<TooltipContent>Toggle calendar</TooltipContent>
			</Tooltip>
			<DropdownMenuContent align="end">
				<DropdownMenuRadioGroup
					value={calendar}
					onValueChange={(value) => setCalendar(value as CalendarPreference)}
				>
					<DropdownMenuRadioItem value="gregorian">
						Gregorian
					</DropdownMenuRadioItem>
					<DropdownMenuRadioItem value="jalali">Jalali</DropdownMenuRadioItem>
				</DropdownMenuRadioGroup>
			</DropdownMenuContent>
		</DropdownMenu>
	);
}
