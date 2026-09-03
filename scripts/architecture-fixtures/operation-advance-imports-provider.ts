// The first import is the violating edge: this module is in the rule's
// from-set, so importing a concrete provider puts one in its reachable graph
// on its own. The second adds the real coordinator's own graph, so the fixture
// stands for the shape the rule exists to forbid - a bounded coordinator that
// ALSO reaches a concrete transport - rather than for a bare provider import.
// The runner cruises this fixture together with the real
// fleet-audit-advance.ts entry and asserts that no module the real coordinator
// reaches at runtime matches this rule's own to-set, enumerated from the rule
// itself. That assertion is load-bearing: this fixture's own violations would
// otherwise mask a new one from the coordinator, because the runner's
// violation-level checks test only rule names and a single to-target.
import '../../packages/fleet-control/src/cloudflare-client.js';
import '../../packages/fleet-control/src/fleet-audit-advance.js';
