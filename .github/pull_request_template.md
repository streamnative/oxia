## Motivation

<!-- The problem, and why it matters. Link the issue, e.g. "Part of #123" or "Fixes #123". -->

## Modifications

<!-- What changed, and how its callers handle it. -->

## Shared state

<!-- Only when the change adds shared mutable state: its owner, its borrowers, and why it could not be avoided.
Remove this section otherwise. -->

## Testing

<!-- The new or changed tests, and what each one checks, and the checks that were run, e.g. `go test -race`,
`go vet` and `golangci-lint`. -->

## Checklist

- [ ] I have read and understood every line of this change, including the code written by AI tools, and I can explain
  why it is correct.
- [ ] The change is covered by tests, or doesn't need any, as explained in Testing.
- [ ] Every commit is signed off (`git commit -s`).
