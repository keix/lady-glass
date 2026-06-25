package pipeline

import "fmt"

func StageKey(jobID string, page int, stage string, version string) string {
	if page > 0 {
		return fmt.Sprintf("%s:page:%06d:%s:%s", jobID, page, stage, version)
	}

	return fmt.Sprintf("%s:%s:%s", jobID, stage, version)
}
