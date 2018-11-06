#!/bin/bash
govendor fetch github.com/SpirentOrion/orion-api/lib/^
govendor fetch github.com/SpirentOrion/orion-api/res/lib/schema/^
govendor fetch github.com/SpirentOrion/orion-client/go/client
govendor fetch -v github.com/SpirentOrion/metrics-service/pkg/metrics/info@0622f715a5985f5b0290854a43d1b803600d9c72
