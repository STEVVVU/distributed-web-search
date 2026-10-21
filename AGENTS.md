# MapReduce runtime

Dependency-free Node 25 ESM service. `npm run check` validates source syntax; `npm test` drives separate real worker processes through HTTP, including crash recovery. Read `.agents/skills/verify-rillmap/SKILL.md`. Keep generated data under ignored `.runs/`. Trusted modules in `jobs/` export `map(key,value)` returning pairs and `reduce(key,values)` returning a JSON value. Do not evaluate remote code. Preserve task fencing and durable publication ordering when changing coordination.
