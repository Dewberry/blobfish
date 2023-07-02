# !/bin/bash

## Script which attempts to make sure that both git and docker are representative of current state of repo, then export the digest
## and commit hashes of the docker repo and git repo, respectively, to environment variables, and finally create the necessary containers
## using a docker compose up

## Usage:
## docker-up.sh docker-image-name

# Check if current git branch has uncommitted changes
echo "Checking git status..."
if [[ $(git status --porcelain) ]]; then
  echo "There are uncommitted changes. Exiting."
  sleep 2 && exit 1
fi

# Check if current git branch has changes not tracked on repo
echo "Checking if remote is up to date..."
if [[ $(git rev-parse HEAD) == $(git rev-parse @{u}) ]]; then
  echo "Up to date"
else
  echo "There are changes not pushed to remote. Exiting."
  sleep 2 && exit 1
fi

# Get git hash URI
echo "Retrieving git hash URI..."
git_hash_uri=$(git config --get remote.origin.url)/commit/$(git rev-parse HEAD)

# Check that git hash is reachable as URL
echo "Testing reachability of git repo..."
git_url=$(echo $git_hash_uri | sed --expression='s/git@/https:\/\//' | sed --expression='s/\.git//' | sed --expression='s/.com:/.com\//')
response=$(curl --write-out %{http_code} --silent --output /dev/null $git_url)
if [[ $response -ne 200 ]]; then
  echo "Error: HTTP status code $response"
  sleep 2 && exit 1
fi

# Pull docker image
echo "Pulling image $1"
docker pull $1

# Get docker digest hash
echo "Getting docker digest hash..."
docker_hash=$(docker inspect --format='{{index .RepoDigests 0}}' $1)

# Export docker image hash
echo "Exporting variables..."
export DOCKER_HASH=$docker_hash

# Export docker image name with default docker repo of docker.io
export DOCKER_IMAGE="docker.io/$1"

# Export docker image digest url with default docker repo of docker.io
export DOCKER_URL="docker.io/$docker_hash"

# Export relative docker file path
export DOCKER_FILE_PATH=$(find . -name "Dockerfile" -type f -printf "%P")

# Export relative docker compose file path
export COMPOSE_FILE_PATH=$(find . -name "docker-compose.yml" -type f -printf "%P")

# Export git hash
export GIT_HASH=$(git rev-parse HEAD)

# Export git repo with hash url
export GIT_REPO=$git_url

# Use compose file to orchestrate docker container creation
echo "Composing containers..."
docker compose up