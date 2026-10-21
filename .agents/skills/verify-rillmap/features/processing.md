# Distributed processing
## Sub-features
Word count, maximum temperature, inverted index, empty inputs and skewed repeated keys.
## How to get to it (user POV)
Install a trusted local job module and submit its name and input pairs to `POST /jobs`.
## Driving it with the Node HTTP harness
`node tools/verify.mjs` submits all cases and compares result pairs to independent sequential oracles.
## Gotchas
All keys are strings; inputs and intermediates must fit the documented bounded JSON budget. Job modules execute trusted code in worker processes.
