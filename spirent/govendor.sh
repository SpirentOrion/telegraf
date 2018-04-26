#!/bin/bash
govendor fetch github.com/SpirentOrion/orion-api/lib/^
govendor fetch github.com/SpirentOrion/orion-api/res/lib/schema/^
govendor fetch github.com/SpirentOrion/orion-client/go/client
govendor fetch -v github.com/SpirentOrion/metrics-service/pkg/metrics/info@=metrics_info
