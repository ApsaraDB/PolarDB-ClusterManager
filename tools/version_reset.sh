#!/bin/sh

rm -f pkg/version/version.go
echo "package version" > pkg/version/version.go
echo "" >>pkg/version/version.go
echo "const GitBranch = \"unknown\"" >> pkg/version/version.go
echo "const GitCommitId = \"unknown\"" >> pkg/version/version.go
echo "const GitCommitDate = \"unknown\"" >> pkg/version/version.go
echo "const GitCommitRepo = \"unknown\"" >> pkg/version/version.go
echo "const BuildUser = \"unknown\"" >> pkg/version/version.go
echo "const BuildHost = \"unknown\"" >> pkg/version/version.go