#!/usr/bin/env bash


# TODO pull in 0.2.0 when supported since it adds support for fixed ints
# go get github.com/leeavital/protoc-gen-gostreamer@v0.1.0

pushd ddsketch/pb/


protoc --plugin=$GOPATH/bin/protoc-gen-gostreamer  --gostreamer_out=. ddsketch.proto

popd


mv ddsketch/pb/github.com/DataDog/sketches-go/ddsketch/pb/sketchpb/*.go  ddsketch/pb/sketchpb/
rm -rf ddsketch/pb/github.com
