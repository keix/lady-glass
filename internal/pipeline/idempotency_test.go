package pipeline_test

import (
	"testing"

	"github.com/keix/lady-glass/internal/pipeline"
)

func TestStageKey(t *testing.T) {
	cases := []struct {
		name    string
		jobID   string
		page    int
		stage   string
		version string
		want    string
	}{
		{
			name:    "page > 0 produces a page-scoped key with 6-digit zero padding",
			jobID:   "job_123",
			page:    17,
			stage:   "line_ocr",
			version: "v1",
			want:    "job_123:page:000017:line_ocr:v1",
		},
		{
			name:    "page == 0 collapses to a job-level key (e.g., merge)",
			jobID:   "job_123",
			page:    0,
			stage:   "merge",
			version: "v1",
			want:    "job_123:merge:v1",
		},
		{
			name:    "version is part of the key so v1 and v2 cannot collide",
			jobID:   "job_123",
			page:    17,
			stage:   "gemini",
			version: "v2",
			want:    "job_123:page:000017:gemini:v2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pipeline.StageKey(tc.jobID, tc.page, tc.stage, tc.version)
			if got != tc.want {
				t.Fatalf("StageKey = %q, want %q", got, tc.want)
			}
		})
	}
}
