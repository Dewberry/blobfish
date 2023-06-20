# !/bin/bash

## Script which attempts to make sure that both git and docker are representative of current state of repo, then export the digest
## and commit hashes of the docker repo and git repo, respectively, to environment variables, and finally create the necessary containers
## using a docker compose up

# Check if current git branch has uncommitted changes
if [[ $(git status --porcelain) ]]; then
  echo "There are uncommitted changes. Exiting."
  sleep 2 && echo 1
fi

# Check if current git branch has changes not tracked on repo
[[ $(git rev-parse HEAD) = $(git rev-parse @{u}) ]] || echo "There are changes not pushed to remote. Exiting." && sleep 2 && exit 1

# Get git hash
git_hash=$(git config --get remote.origin.url)/commit/$(git rev-parse HEAD)

# Check that git hash is reachable as URL
git_url=$(git_hash | sed 's/git/https/;s/:/\//' | sed 's/\.git$//')
response=$(curl --write-out %{http_code} --silent --output /dev/null $git_url)
if [ $response -ne 200 ]; then
  echo "Error: HTTP status code $response"
  sleep 2 && exit 1
fi

# Push the state of the repo to the image
docker push $1

# Get docker digest hash
docker_hash=$(docker inspect --format='{{index .RepoDigests 0}}' $1)

# Export docker image hash
export DOCKER_HASH=$docker_hash

# Export git hash
export GIT_HASH=$git_hash

# Use compose file to orchestrate docker container creation
docker compose up