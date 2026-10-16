package constant

import "os"

// AsterGateEnabled is opt-in per deployment; restart after changing the environment.
// Keep this out of persisted settings so removing the extension needs no migration.
func AsterGateEnabled() bool { return os.Getenv("ENABLE_ASTERGATE") == "true" }

// ChannelTypeAsterGate is a local extension ID, outside the upstream sequential
// allocation. Keep it stable: persisted channels refer to this number.
const ChannelTypeAsterGate = 1001
