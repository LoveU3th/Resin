// Package buildinfo resolves version information from ldflags and Go VCS metadata.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

const (
	developmentVersion = "dev"
	unknownGitCommit   = "unknown"
	shortCommitLength  = 8
)

// These values may be set via -ldflags at build time:
//
//	go build -ldflags "-X github.com/Resinat/Resin/internal/buildinfo.Version=1.0.0 ..."
var (
	Version   string
	GitCommit string
	BuildTime = "unknown"
)

type vcsInfo struct {
	revision string
	modified bool
}

func init() {
	Version, GitCommit = resolve(Version, GitCommit, readVCSInfo())
}

func readVCSInfo() vcsInfo {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return vcsInfo{}
	}

	var vcs vcsInfo
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			vcs.revision = strings.TrimSpace(setting.Value)
		case "vcs.modified":
			vcs.modified = setting.Value == "true"
		}
	}
	return vcs
}

func resolve(explicitVersion, explicitCommit string, vcs vcsInfo) (string, string) {
	version := strings.TrimSpace(explicitVersion)
	commit := strings.TrimSpace(explicitCommit)
	hasExplicitCommit := commit != "" && commit != unknownGitCommit
	vcsCommit := shortCommit(vcs.revision)

	if !hasExplicitCommit {
		commit = vcsCommit
	}
	if commit == "" {
		commit = unknownGitCommit
	}

	if version != "" {
		return version, commit
	}
	if hasExplicitCommit {
		return commit, commit
	}
	if vcsCommit != "" {
		version = vcsCommit
		if vcs.modified {
			version += "-dirty"
		}
		return version, commit
	}
	return developmentVersion, commit
}

func shortCommit(revision string) string {
	revision = strings.TrimSpace(revision)
	if len(revision) <= shortCommitLength {
		return revision
	}
	return revision[:shortCommitLength]
}
