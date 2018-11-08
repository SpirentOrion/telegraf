#!/bin/bash
govendor fetch github.com/SpirentOrion/orion-api/lib/^
govendor fetch github.com/SpirentOrion/orion-api/res/lib/schema/^
govendor fetch github.com/SpirentOrion/orion-client
govendor fetch github.com/SpirentOrion/metrics-service/pkg/metrics/info
govendor fetch github.com/SpirentOrion/luddite
