CREATE TABLE `dids` (
	`did` text PRIMARY KEY NOT NULL,
	`method` integer NOT NULL,
	`deactivated` integer DEFAULT false,
	`pds` text,
	`ts` integer
);
