# Releasing binaries from vector-store repositiory

## Releasing vector-store

You need to have qemu plus docker support for multi-platform builds. See
https://docs.docker.com/build/building/multi-platform/. You can use
`scripts/prepare-docker-qemu` script to register qemu handlers in docker
engine.

You need to git clone repository and switch to the desired version tag. Then
from the root of the repository run:

```bash
./scripts/build-release
```

This will create a tar.gz release in the `releases/{amd64/arm64}` directory and
multi-platform docker image for linux with version tag. You can push the docker
image to docker hub using standard docker commands.

