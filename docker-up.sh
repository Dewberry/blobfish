# !/bin/bash

## Script which attempts to make sure that the current commit hash can be found on

# Check if current git branch has uncommitted changes
if [[ $(git status --porcelain) ]]; then
  echo "There are uncommitted changes. Exiting."
  sleep 2 && echo 1
fi

# Check if current git branch has changes not tracked on repo
[[ $(git rev-parse HEAD) = $(git rev-parse @{u}) ]] || echo "There are changes not pushed to remote. Exiting." && sleep 2 && exit 1
if

# Get git hash
git_hash=$(git config --get remote.origin.url)/commit/$(git rev-parse HEAD)

# Check that git hash is reachable as URL
git_url=$(git_hash | sed 's/git/https/;s/:/\//' | sed 's/\.git$//')
response=$(curl --write-out %{http_code} --silent --output /dev/null $git_url)
if [ $response -ne 200 ]; then
  echo "Error: HTTP status code $response"
  sleep 2 && exit 1
fi

# Export docker image hash
export DOCKER_HASH=$(docker inspect --format='{{index .RepoDigests 0}}' $1);

# Export git hash
export GIT_HASH=$git_hash;

# Use compose file to orchestrate docker container creation
docker compose up