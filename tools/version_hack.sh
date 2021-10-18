#!/bin/sh

commit_id=$(git rev-parse HEAD)
echo "commitId: $commit_id"

commit_branch=$(git symbolic-ref --short -q HEAD)
echo "branch $commit_branch"

commit_date=$(git log -1 --format="%cd")
echo "git commit date: $commit_date"


gitrepo=$(git remote -v|grep origin|grep fetch|awk '{print $2}')
echo "git repo: $gitrepo"

buildUser=$(whoami)
echo "buildUser: $buildUser"

BUILDDATE=$(date '+%Y%m%d%H%M%S')

buildHost=$(hostname)
echo "buildHost: $buildHost"

rm -f pkg/version/version.go
mkdir -p pkg/version
echo "package version" > pkg/version/version.go
echo "" >>pkg/version/version.go
echo "const GitBranch = \"$commit_branch\"" >> pkg/version/version.go
echo "const GitCommitId = \"$commit_id\"" >> pkg/version/version.go
echo "const GitCommitDate = \"$commit_date\"" >> pkg/version/version.go
echo "const GitCommitRepo = \"$gitrepo\"" >> pkg/version/version.go
echo "const BuildDate = \"$BUILDDATE\"" >> pkg/version/version.go
echo "const BuildUser = \"$buildUser\"" >> pkg/version/version.go
echo "const BuildHost = \"$buildHost\"" >> pkg/version/version.go
