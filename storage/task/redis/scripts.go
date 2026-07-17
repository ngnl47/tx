package redis

import _ "embed"

//go:embed scripts/enqueue.lua
var enqueueScript string

//go:embed scripts/claim.lua
var claimScript string

//go:embed scripts/complete.lua
var completeScript string

//go:embed scripts/retry.lua
var retryScript string
