# Contributing

Code contributions are always welcome! 

* Open an issue in the repo with defect/enhancements
* We can also be reached @ https://gitter.im/expedia-haystack/Lobby
* Fork, make the changes, build and test it locally
* Issue a PR - watch the PR build in [travis-ci](https://travis-ci.org/ExpediaDotCom/haystack-traces)
* Once merged to master, travis-ci will build and release the artifacts to [docker hub]


## Building

####Prerequisite: 

* Make sure you have Java 1.8
* Make sure you have maven 3.3.9 or higher
* Make sure you have docker 1.13 or higher


Note : For mac users you can download docker for mac to set you up for the last two steps.

####Build

For a full build, including unit tests and integration tests, docker image build, you can run -
```
make all
```

####Integration Test

####Prerequisite:
1. Install docker using Docker Tools or native docker if on mac
2. Verify if docker-compose is installed by running following command else install it.
```
docker-compose

```

Run the build and integration tests for individual components with
```
make indexer

```

&&

```
make reader

```


```
make backends

```


## Releasing the artifacts

Currently we publish the repo to docker hub and nexus central repository.

* Git tagging: 

```
git tag -a <tag name> -m "Release description..."
git push origin <tag name>
```

`<tag name>` must follow semantic versioning scheme.

Or one can also tag using UI: https://github.com/ExpediaDotCom/haystack-traces/releases

It is preferred to create an annotated tag using `git tag -a` and then use the release UI to add release notes for the tag.

* After the release is completed, please update the `pom.xml` files to next `-SNAPSHOT` version to match the next release