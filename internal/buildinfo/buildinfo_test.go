package buildinfo

import "testing"

func TestResolve(t *testing.T) {
	t.Parallel()

	const revision = "9b8ef8e5cf83071fbac4de29bd7187268b9cff7b"
	tests := []struct {
		name            string
		explicitVersion string
		explicitCommit  string
		vcs             vcsInfo
		wantVersion     string
		wantCommit      string
	}{
		{
			name:            "explicit release metadata wins",
			explicitVersion: "v1.2.0",
			explicitCommit:  "abcdef12",
			vcs:             vcsInfo{revision: revision, modified: true},
			wantVersion:     "v1.2.0",
			wantCommit:      "abcdef12",
		},
		{
			name:            "explicit version fills missing commit from vcs",
			explicitVersion: "custom",
			vcs:             vcsInfo{revision: revision, modified: true},
			wantVersion:     "custom",
			wantCommit:      "9b8ef8e5",
		},
		{
			name:           "explicit commit becomes default version",
			explicitCommit: "abcdef12",
			vcs:            vcsInfo{revision: revision, modified: true},
			wantVersion:    "abcdef12",
			wantCommit:     "abcdef12",
		},
		{
			name:        "clean vcs revision",
			vcs:         vcsInfo{revision: revision},
			wantVersion: "9b8ef8e5",
			wantCommit:  "9b8ef8e5",
		},
		{
			name:        "modified vcs revision",
			vcs:         vcsInfo{revision: revision, modified: true},
			wantVersion: "9b8ef8e5-dirty",
			wantCommit:  "9b8ef8e5",
		},
		{
			name:            "explicit dev remains explicit",
			explicitVersion: "dev",
			vcs:             vcsInfo{revision: revision, modified: true},
			wantVersion:     "dev",
			wantCommit:      "9b8ef8e5",
		},
		{
			name:           "legacy unknown commit falls back to vcs",
			explicitCommit: unknownGitCommit,
			vcs:            vcsInfo{revision: revision},
			wantVersion:    "9b8ef8e5",
			wantCommit:     "9b8ef8e5",
		},
		{
			name:        "short revision",
			vcs:         vcsInfo{revision: "abc123"},
			wantVersion: "abc123",
			wantCommit:  "abc123",
		},
		{
			name:        "missing metadata",
			wantVersion: developmentVersion,
			wantCommit:  unknownGitCommit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotVersion, gotCommit := resolve(tt.explicitVersion, tt.explicitCommit, tt.vcs)
			if gotVersion != tt.wantVersion {
				t.Fatalf("version = %q, want %q", gotVersion, tt.wantVersion)
			}
			if gotCommit != tt.wantCommit {
				t.Fatalf("commit = %q, want %q", gotCommit, tt.wantCommit)
			}
		})
	}
}
