#!/bin/bash

tmpdir="$(mktemp -d)"
git clone --depth=1 --quiet git://github.com/apache/arrow "$tmpdir"
git --git-dir="$tmpdir/.git" rev-parse HEAD
