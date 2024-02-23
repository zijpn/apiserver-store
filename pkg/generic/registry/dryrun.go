package registry

func isDryRun(flag []string) bool {
	return len(flag) > 0
}