export const PgEnvSchema = {
	type: "enum",
	optional: false,
	values: ["development", "production"],
	default: "development",
};
