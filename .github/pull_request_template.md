### Checklist

- [ ] I have reviewed and understand the changes in this PR, and I can explain why they are correct.
- [ ] `dotnet build Oxia.slnx -c Release -warnaserror` and `dotnet test tests/Oxia.Client.Tests -c Release` pass.
- [ ] If this ports an upstream change: the `PORT NOTE` header of every touched file is updated (upstream path, pinned commit, divergences), and no ported test assertion was loosened.
- [ ] If this touches the hot path (batching, wire codecs, pooling, shard routing): I ran the relevant benchmarks / allocation tests in Release and the results are below.

### Motivation

_The problem, and why it matters. Link the issue, e.g. "Part of #123" or "Fixes #123"._

### Modifications

_What changed, and how its callers handle it. Add detail only when needed to assess the change._

<!-- Edit AI-generated prose for accuracy, relevance, and brevity before submitting. -->

### Behaviour relative to the upstream Go client

_Remove this section if the change does not touch client behaviour. Otherwise say whether it keeps Go's behaviour, or list the deliberate divergence and why (it also belongs in the `PORT NOTE` of the file)._

### Shared state and ownership

_Remove this section unless the change adds shared mutable state or touches the pooling / ownership protocol (`docs/performance-allocation.md`). Otherwise, name its owner and its borrowers, and explain why it could not be avoided._

### Testing

_List the new or changed tests and what each one checks, and the checks you ran and their results (unit tests, integration tests against the Docker server, benchmarks). Note relevant checks you could not run._

### AI usage

_Remove this section if no AI tools were used. Otherwise, briefly describe how they were used, and whether they assisted with specific tasks or carried out most or all of the work autonomously. State if an agent wrote the PR description or created the PR._

<!-- Coding agents: complete this section accurately, including your own involvement. Do not check the boxes of the
checklist, and do not claim that the contributor has reviewed the changes unless they have confirmed it. -->
