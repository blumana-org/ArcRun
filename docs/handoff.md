# Handoff — État courant et reprise

Contexte de reprise pour la prochaine session.

**Correctif du 2026-10-09, audit point 3** : ordre tâches → batch → slots,
au lieu de slots → tâches pour le claim/flush. Le claim combine le verrou de tâche
et le premier upsert dans une CTE ; pas d'aller-retour supplémentaire ni d'UPDATE
spéculatif de tâche en cas de règle bloquante. Arrêt de batch et édition de règles
préverrouillent leurs ensembles ; GC des slots trié et `SKIP LOCKED`.
Régressions déterministes : `test_bug_audit6`. Mesures et limites : fin de
`docs/perf-correctness-plan.md`. Les cascades gardent leur parcours par frontière ;
aucune garantie générale d'absence de deadlock ni retry générique ajouté.

## Campagne en cours : fixes Audit 2 (depuis 2026-07-08)

**Avancement au 2026-07-09** : **Lots 4 et 5 terminés** — Lot 4 (4.1 → 4.8,
dernier commit 01d034f) ; Lot 5 : 5.1 (A5 SSRF, 36b6f7a), 5.2 (A6 auth,
c3bee32), 5.3 (A10 limites, eaf67f6). **Lot 6 TERMINÉ** : 6.1 (B1, 1623934),
6.2 (B3, bd64090), 6.3 (B5, aac34c0), 6.4 (B4, 395b9a2), 6.5 (B2, 21f38bc),
6.6a (B6/B7, ceb6ea3), 6.6b (index morts + idx_task_priority + selects
ciblés + timeout_loop borné + probes 2 s + circuit breaker opérant).
**La campagne non-breaking (Lots 4/5/6) est CLOSE.** Le **Lot 7 (breaking)
est partiellement ouvert** sur décision utilisateur du 2026-07-09 : périmètre
= **7.2 puis 7.3 uniquement** (7.1 API v1/HMAC, 7.4 GET /work, 7.5 split
outbox restent non décidés). **7.2 (D2, `batch.remaining`) : FAIT** (525fd9a) —
compteur dénormalisé remplaçant le FOR UPDATE + NOT EXISTS (et l'index
partiel B3, droppé), décrément groupé en-tx à chaque site terminal,
`remaining` exposé dans GET /batches. **7.3a (D1/D7 Concurrency) : FAIT** —
`rule_slot` + `task.claimed_slot_keys` (libération aux 7 sites de sortie de
Claimed/Running, dont requeue-stale), leader-lease advisory sur la
start_loop (connexion dédiée, failover). Sémantique assumée : seuls les
porteurs de la règle comptent dans le slot. **7.3b (D1 Capacity) : FAIT**
(2026-07-10) — Capacity sur `rule_slot` aussi : `task.capacity_charge`
consommée au claim (upsert unifié conc+cap, garde `max_capacity <= 0`),
rétrécie de façon monotone par le flush du batch_updater (pré-lock A9
tasks-avant-slots depuis le correctif du 2026-10-09), libérée par le release généralisé (`cap:` ⇒ charge
stockée). Advisory + probe CTE-SUM supprimés ; divergence assumée : le
PATCH compteur direct ne pousse pas de delta (sur-comptage conservateur,
réconcilié au release). **Le périmètre décidé du Lot 7 (7.2 + 7.3) est
TERMINÉ**. Décision utilisateur du 2026-07-10 : lancement du **7.5**
(D3/D6), périmètre = 7.5a table `webhook_outbox` dédiée + 7.5b
`task_archive` (dispatcher séparé écarté, partitioning hors périmètre).
**7.5a (D3) : FAIT** — queue de livraison dans `webhook_outbox` (pas de
colonne status ; au terme de la livraison la ligne est supprimée et
historisée dans `webhook_execution` en une CTE atomique) ;
`webhook_execution` = ledger on_start + historique ; backstop
anti-re-signal `NOT EXISTS (ledger)` à l'enqueue ; gates A2/D2 et
contrat GET /webhook-deliveries inchangés. **7.5b (D6) : FAIT** — la
rétention déplace les tâches terminales vers `task_archive` (move
atomique, colonnes explicites, aucune FK) au lieu de les supprimer ;
`GET /task/{id}` sert l'historique en fallback (actions `[]`), les
écritures restent 404 ; purge optionnelle `RETENTION_ARCHIVE_DAYS`
(défaut 0 = conserver pour toujours). **L'item 7.5 est TERMINÉ.**
7.1 (API v1/HMAC) et 7.4 (GET /work) restent non décidés — ne rien
lancer sans décision utilisateur.
Détail et notes de relecture par item dans
`docs/audits/AUDIT_2_FIXES.md`. Décisions actées : pause = Pending+Waiting,
Running → 400 (4.5) ; doc metadata = replace, merge → Lot 7 (4.7) ; renversement
assumé du commit 2e50620 en 4.4 (compteurs terminaux gelés) ; HMAC du `?handle=`
→ Lot 7 (5.2) ; auth non-breaking (token absent ⇒ ouvert + warn release, 5.2) ;
limites 5.3 = 1000 tâches/batch, 100 deps/tâche, 20 actions/tâche, payload 2 MiB
(env-overridables) ; DFS de cycles supprimé (forward-reference rule prouvée).

- **Référence d'analyse** : `docs/audits/AUDIT_2_CLAUDE.md` — audit complet perf,
  correctness et architecture (post Lots 0-3). Chaque finding : fichier:ligne, scénario
  d'échec, fix suggéré, sévérité. Findings majeurs contre-vérifiés à la main.
- **Suivi d'exécution** : `docs/audits/AUDIT_2_FIXES.md` — tableau des items (Lots 4-7),
  process de la boucle, prompt de reprise, journal. **C'est là qu'on lit l'état et
  qu'on le met à jour.**
- **Rôles** : implémentation déléguée à un agent **Opus 4.8** (Agent tool, `model: opus`)
  avec prompt détaillé ; **relecture indépendante par Fable** (session principale) :
  diff complet, invariants, re-run `cargo check` + suites ciblées + régressions
  (`tests/integration/test_bug_audit3.rs`). Ne pas se fier au rapport de l'implémenteur.
- Ordre : Lot 4 (correctness critique) → Lot 5 (sécurité) → Lot 6 (perf non-breaking).
  Le Lot 7 (breaking : API v1, batch.remaining, rule_slot, GET /work) ne se lance que
  sur décision explicite de l'utilisateur.

## Historique

- **Plan perf & correctness (Lots 0-3) : TERMINÉ** (2026-06-12, committé). Référence de
  design et décisions actées : `docs/perf-correctness-plan.md` — à lire avant de toucher
  claim loop / outbox / insertion groupée / batch-complete, pour ne pas régresser une
  décision déjà tranchée (LIMIT-less visibility, on_start synchrone = control-flow,
  pas de CTE récursive, enqueue outbox inconditionnel…).
- Audit 1 (2026-02-14) : `docs/audits/AUDIT_1_*.md`, régressions dans
  `tests/integration/test_bug_audit{1,2}.rs`.

## Process de travail (à reconduire)

1. L'implémentation est déléguée à un agent Opus (prompt : section du plan/audit copiée,
   fichiers, invariants CLAUDE.md, tests exigés).
2. La session principale fait la relecture indépendante (diff complet, invariants,
   re-run des tests — jamais sur la seule foi du rapport de l'agent).
3. Mise à jour du tableau de suivi + commit à chaque item (staging explicite).

## Pièges d'environnement connus

- **Tests d'intégration** : cible unique — `cargo test --test integration <filtre>`
  (ex. `outbox`, `claim_loop`, `priority`). Les fichiers sont des modules déclarés dans
  `tests/integration/main.rs`. Testcontainers exige Docker et l'image `postgres:18-alpine`
  (la pré-puller si le premier run timeout : un timeout de création de conteneur
  empoisonne le `LazyLock` partagé et fait échouer toute la suite).
- **Docker Desktop fragile** : un disque plein l'a fait crasher en laissant un
  `com.docker.backend` zombie qui survit au SIGTERM et fait croire à `open -a Docker`
  que tout tourne. Remède : `kill -9 <pid backend>` puis `open -a Docker`.
- **Espace disque** : les builds cargo remplissent vite le disque. En cas de
  `No space left on device` : `rm -rf ~/.cargo/target/debug/incremental`, ou
  `cargo clean` si insuffisant (rebuild complet ensuite).
- **Working tree partagé** : une autre session peut travailler sur `ui/`. Toujours
  **stager explicitement** les fichiers de son lot, jamais `git add -A`. Ne jamais
  toucher `static/dag.html` ni `ui/`.
- **Commits** : pas de trailer Co-Authored-By.
