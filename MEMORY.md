## .gitattributes for TSV files
- Added `.gitattributes` with `*.tsv linguist-generated=true`
- 7 DEPENDENCIES.rust.tsv files (~2900 lines total) are auto-generated
- This collapses them by default in GitHub PR diffs

## Apache Release Discussion Email Research

### Key Findings
- Paimon (Java) uses simple [DISCUSS] emails: just states time since last release, mentions some PRs, asks for blockers
- OpenDAL has a formal template in its release guide at opendal.apache.org/community/release/
- Arrow-rs uses informal [DISCUSS] emails, e.g. "[DISCUSS] Rust 57.3.0 release" by Andrew Lamb
- No [DISCUSS] email has been sent yet for paimon-rust 0.1.0 release
- Kvrocks has no [DISCUSS] template, only [VOTE] template

### OpenDAL [DISCUSS] Template (Canonical)
```
Subject: [DISCUSS] Release Apache OpenDAL ${release_version}

Hello, Apache OpenDAL Community,

This is a call for a discussion to release Apache OpenDAL version ${opendal_version}.

The change lists about this release:
https://github.com/apache/opendal/compare/v${opendal_last_version}...main

Please leave your comments here about this release plan.
We will bump the version in the repo and start the release process after the discussion.

Thanks
${name}
```

### Paimon (Java) [DISCUSS] Style (Informal)
- Subject: [DISCUSS] Release X.Y or [DISCUSS] Release X.Y.Z
- Body: mentions time since last release, references notable PRs, asks for blockers
- Example by Jingsong Li for 1.1: "It has been 2-3 months since we released version 1.0..."

### Process Flow
1. [DISCUSS] email on dev@ -> collect feedback / blockers
2. Bump version, create RC tag
3. [VOTE] email on dev@ -> 72h minimum, 3 +1 binding votes needed
4. [RESULT][VOTE] email -> announce results
5. For incubating projects: additional vote on general@incubator.apache.org
