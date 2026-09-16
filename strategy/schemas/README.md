# OOF offline Strategy contract schemas

These schemas implement the initial offline subset of `noop.strategy/v1`. They
describe complete accepted wire shapes for this subset. They do not implement
the account ledger, a Strategy sandbox, live risk admission, signing, custody,
venue capability certification, fees, or customer withdrawals.

OOF is the shared V3 platform; Noop is one Strategy on it. The existing
`noop.strategy/v1` wire name and its pinned metadata remain compatibility
identifiers. This naming change does not rewrite schema constants or accepted
release digests. A future namespace change requires a versioned migration.

`release.json`, `mandate.json`, `input-bundle.json`, and `decision.json` reference
shared definitions in `common.json`. `../contract.js` validates these exact JSON
Schema documents, then applies cross-object and economic-shape constraints.
There is no independently maintained parallel list of structural fields.

The validator supports this JSON Schema 2020-12 subset: `$schema`, `$id`, `$defs`,
local `$ref`, `description`, `type`, `const`, `enum`, `properties`,
`propertyNames`, `required`, `additionalProperties`, `minProperties`,
`maxProperties`, `items`, `minItems`, `maxItems`, `uniqueItems`, `minLength`,
`maxLength`, `pattern`, `format`, `minimum`, `maximum`, `anyOf`, and `oneOf`.
Only the custom `utc-milliseconds` format is supported. Unrecognized schema
keywords and unresolved references fail at module load; they are not ignored.
No schema or reference is fetched from the network. External schema tooling must
register the custom format, or enforce the timestamp rule separately.

## Encoding and identity

- Timestamps are valid Gregorian UTC calendar times with exactly three
  fractional digits: `2030-01-01T12:00:00.000Z`.
- Economic numbers are decimal strings with an optional minus sign, no leading
  integer zeroes, no exponent, at most 60 integer digits and 30 fractional
  digits. Each typed amount or parameter declaration supplies a registered
  unit. Semantic rules reject negative quantities and caps where appropriate.
- Decimal string scale is preserved in identity: `"0.10"` and `"0.1"` have
  equal numeric value but different serialized content and content digests.
  Arithmetic compares their exact values without binary floating point.
- JSON numbers are finite safe integers; negative zero is rejected. Object
  names sort by UTF-16 code units, arrays retain order, and strings use JSON
  escaping. The canonical form contains no insignificant whitespace.
- Only plain JSON structures are accepted. Accessors, hidden properties,
  inherited custom prototypes, sparse arrays, cycles, unsupported values, lone
  Unicode surrogates, and unsafe prototype keys are rejected. Structural,
  recursion, and cumulative byte limits are control-plane capacity limits.
- `contentDigest(value)` computes `sha256:` followed by lowercase SHA-256 hex
  over UTF-8 canonical JSON. A release hashes the full manifest **excluding**
  `release_digest`; the manifest separately pins the runtime artifact and
  dependency-lock digests. An input bundle hashes itself **excluding**
  `input_bundle_id`. Digests establish content identity, not customer consent
  or an artifact publisher's signature.

## Initial supported surface

Every parameter is an explicitly declared decimal with a unit, range, default,
and description. Every mandate materializes the exact declared parameter set.
There are no implicit platform defaults for 25-DTE put rolling, 80% call-profit
capture, or 45% call margin. Release-specific combination and behavioral checks
belong to that release's policy validator.

The release declares fixed runtime resource bounds and action, venue,
instrument-kind and spot-route capabilities. Rich dataset manifests, external
research capability grants, invocation scheduling, and private-state schemas
remain future work. `extensions` and intent `evidence` are bounded opaque JSON
metadata; they supply no execution authority and are not interpreted by this
contract validator. Strategy-specific code must not treat them as customer
acceptance of undeclared economic permissions.

The account identity includes network, chain ID, deployment, owner, venue,
decimal-string `subaccount_id`, decimal-string `manager_id`, and risk universe.
The manager ID is an identifier, not an assumed contract address.

An input bundle separates account-scoped `fields` from `instrument_fields`.
Each latter map is keyed by an accepted `instrument_ref` or spot `route_ref` and
contains only catalog entries scoped to that candidate. Consumers resolve
only the current intent's map. A quote or position statistic for one option
cannot be reused as another option's evidence. Observation and availability
times cannot exceed the pinned evaluation clock; absent or invalid values are
represented explicitly. Inputs do not certify their own provenance or live
account reconciliation.

Intents identify one concrete option or spot route. Candidate lists, package
orders, transfers, withdrawal destinations, and arbitrary executable condition
code are unsupported. Buy orders need a finite total outlay cap that covers
maximum normalized quantity times the limit price plus maximum fees. Price
units are `USDC/contract` for normalized options and `USDC/ETH` for spot; this
contract does not apply the contract multiplier again. All actions need bounded
fees and lifetime quantity. Sell calls require a named capacity reference and
a finite proposed stressed-liability cap. **This cap does not make a short
call's terminal loss bounded:** a future authoritative risk gate must enforce
the accepted scenario definition, collateral, margin, and inventory policy.

Put sales and call buybacks require identified inventory and `reduce_only:
true`; accepting this request does not prove a venue supports equivalent
close-only execution. Spot actions likewise require a declared route but remain
proposals until the deployment has a separately certified delivery and custody
adapter. A declaration alone never enables either capability.

Decision validation checks accepted identifiers, revisions, mandate authority,
input identity, private-state compare-and-swap, action semantics, typed
conditions, finite order bounds, and dependency graphs against supplied
context. To perform all cross-object checks, pass the release, mandate, input
bundle, and control state. Calling without those objects performs only the
checks supported by the supplied context; it is not acceptance. The control
module owns immutable identifier replay and control-state transitions.

Stored-intent validation preserves expired instructions and prior revisions for
reconciliation. New upserts require an active mandate; cancellation and
no-action decisions remain available while ordinary entry authority is
paused. The eventual customer exit worker has separate lifecycle authority.
All retained intents appear in the input's `active_intents` control projection,
including cancellation and replacement states; that name does not mean every
row is executable.

Dependencies reference an exact predecessor revision and an accounted fill
threshold, cancellation reconciliation, or a named ledger event. Cycles,
self-reference, unavailable revisions, and incompatible quantities reject.
Acknowledgement is not a supported dependency condition. Atomic acceptance of
several intents never implies atomic orders or financial reservation.
