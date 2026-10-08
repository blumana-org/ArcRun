# Plan Perf & Correctness — ArcRun

Issu de l'audit de performance du 2026-06-11. Ce document est la référence de cohérence
entre les lots : il consigne les designs **et les raisons** des choix, pour ne pas
réintroduire un problème déjà tranché.

## Mise à jour du 2026-10-08 — workers bornés, résultats au fil de l'eau

Les workers start/delivery remplissent leurs places libres sans attendre la fin
d'un lot. Une place reste occupée jusqu'à la fin du traitement du résultat. Les
claims ne dépassent pas les places libres : les tâches en attente restent Pending,
et les événements non pris restent dans l'outbox. Cela supprime la file de Claimed
dont seul le premier élément en attente recevait un heartbeat.

Delivery persiste les résultats prêts avant de reprendre du travail, partage une
connexion entre ces écritures et le prochain claim/préchargement, et conserve les
résultats en cas d'erreur de pool ou SQL. Leur nombre reste borné par la concurrence.
Les tokens protègent chaque écriture, y compris après reprise d'une lease par un
autre worker. Un crash entre HTTP et écriture peut toujours provoquer un doublon.
Les transactions d'admission, quotas, transitions et enqueue ne changent pas.

Le premier scan du scheduler lit seulement le nombre de candidats nécessaire aux
places libres. Si ces candidats sont bloqués, la pagination reprend avec de grandes
pages, sans masquer les tâches éligibles plus loin dans le backlog. Les résultats
d'exclusion de règles restent limités au scan courant.

Mesure locale comparative avec PostgreSQL 18, 64 tâches, concurrence 4, limite de
claim 8, polling 5 ms, pool de 5 connexions, profil Rust test (`opt-level=1`). Les
temps ci-dessous sont les médianes de trois essais, de l'activation du worker à la
persistance des 64 résultats, hors création des tâches. Les exécutables avant
(`0447ac8`) et après sont compilés séparément puis exécutés sans compilation de
l'app pendant la mesure. L'entrée start mesurée est celle sans élection de leader.

| Scénario | Avant | Après |
|----------|------:|------:|
| Delivery, HTTP 1 ms | 170 ms | 164 ms |
| Delivery, 1 HTTP de 200 ms tous les 8 événements, autres 1 ms | 1 824 ms | 552 ms |
| Start, HTTP 1 ms | 247 ms | 178 ms |
| Start, même mélange rapide/lent | 1 819 ms | 587 ms |

Le petit écart sur delivery rapide est dans une plage où le bruit de mesure compte.
Ces chiffres ne prédisent pas le débit en production. Les prises de connexion,
claims et préchargements peuvent être plus fréquents qu'avec de gros lots ; le
scheduler de production vérifie aussi sa connexion de leader avant chaque refill.
Une base distante ou un gros préfixe de tâches bloquées demandent donc une mesure
représentative. Le gain attendu vient surtout des places qui restent occupées en
présence d'appels lents et de l'absence d'attente du polling entre lots.

## Contrat API cible (décisions actées)

1. **Réponse API = état durable.** Quand `PATCH /task/{id}` répond 200, la transition de
   statut et toute la propagation aux enfants sont committées.
2. **Webhooks = notifications at-least-once.** Tout événement de cycle de vie (end, cancel)
   sera livré au moins une fois, y compris après crash/redéploiement. Dédoublonnage côté
   consommateur via le header `Idempotency-Key`. Pas d'ordre garanti **entre tâches**
   (le `on_success` du parent peut arriver après le `on_start` de l'enfant — l'ordre causal
   vit dans l'état, pas dans les notifications). Ordre garanti **par tâche** : `start` livré
   avant `end`.
3. **L'état est la vérité.** Un consommateur qui rate un webhook réconcilie via le
   `?handle=` (GET /task/{id}). Les états terminaux sont immuables.
4. **`on_start` est du control-flow, pas une notification** : sa réponse pilote
   l'orchestrateur (action cancel, échec ⇒ tâche Failed). Il reste synchrone dans la
   `start_loop` et ne passera PAS par l'outbox.

---

## Lot 0 — Quick wins — ✅ FAIT (2026-06-11, dans le working tree)

1. `src/main.rs` : un seul `ActionExecutor` partagé (workers + AppState) — un seul pool reqwest.
2. `src/workers/start_loop.rs` : logs « blocked by rule » passés de `warn` à `debug`
   (les métriques Prometheus `record_task_blocked_by_concurrency` restent).
3. `src/db/task_query.rs` : filtres `LIKE` de `list_task_filtered_paged` appliqués
   seulement si non vides (colonnes NOT NULL ⇒ équivalent).
4. Migration `2026-06-11-000001_drop_redundant_status_index` : drop de `idx_task_status`
   (couvert par `idx_task_priority(status,…)` et `idx_task_status_kind(status,kind)`).
   ✅ Validé par `EXPLAIN ANALYZE` (2026-06-11, Postgres 18, 200k lignes seedées, ANALYZE) :
   `find_timed_out_tasks` et `requeue_stale_claimed_tasks` font un Bitmap Index Scan sur
   `idx_task_status_kind` (le planner la préfère à `idx_task_priority` prédite — les deux
   sont status-leading, conclusion identique : aucun seq scan), et la page Pending de la
   `start_loop` utilise `idx_task_priority`. Rollback : `down.sql`.

---

## Lot 1 — Boucle de claim paginée — ✅ FAIT (2026-06-11)

### Problème
`list_all_pending` (`src/db/task_query.rs`) charge **toutes** les tâches Pending (lignes
complètes avec JSONB) à chaque itération (1 s) : le `.limit()` a été retiré volontairement,
et `_batch_size` dans `claim_phase` (`src/workers/start_loop.rs`) est ignoré.

### Pourquoi pas un simple LIMIT (décision actée — NE PAS régresser)
Head-of-line blocking : avec `LIMIT 50`, si les 50 premières tâches (par priorité/ancienneté)
sont bloquées par une règle de concurrence, une 51e tâche éligible ne serait **jamais** vue.
C'est la raison pour laquelle le LIMIT avait été retiré.

### Design retenu : ne pas limiter la visibilité, limiter la mémoire
```
claim_cap  = WORKER_START_BATCH_SIZE  (config existante, redevient utile : cap de claims/itération)
page_size  = constante interne (~500)  (borne la mémoire)

cursor = début
loop:
    page = Pending WHERE keyset > cursor ORDER BY priority DESC, created_at ASC, id ASC LIMIT page_size
    pour chaque tâche : prefilter ko-cache → claim_task_with_rules → claimed++
    si claimed >= claim_cap : break    # on s'arrête parce qu'on a du travail, pas par aveuglement
    si page incomplète : break          # backlog entièrement parcouru
    cursor = dernière ligne de la page
```
- L'arrêt anticipé ne se déclenche **que** si on a réellement claim `claim_cap` tâches.
  Sinon on parcourt tout le backlog ⇒ aucune famine possible.
- Le cache `ko` (lock keys bloquées) est conservé **entre les pages** d'une même itération :
  scanner 50k tâches bloquées ≈ ~100 requêtes de pagination indexées + 1 check DB par classe
  de règle.
- **Keyset, pas OFFSET.** Attention à l'ordre mixte (`priority DESC, created_at ASC, id ASC`) :
  pas de comparaison de tuple directe en SQL, il faut le prédicat développé :
  `(priority < p) OR (priority = p AND created_at > c) OR (priority = p AND created_at = c AND id > i)`.
  Sert l'index `idx_task_priority(status, priority DESC, created_at ASC)`.
  Le tiebreaker `id` rend le curseur stable si des tâches sont claim entre deux pages.

### Batch-claim des tâches sans règles (dans une page)
Les runs **contigus dans l'ordre de priorité** de tâches avec `start_condition` vide sont
claim en un seul `UPDATE task SET status='claimed', last_updated=now()
WHERE id = ANY($ids) AND status='pending' RETURNING id`.

⚠️ Garde-fou (décision actée) : ne PAS batcher au-delà d'une tâche porteuse de règles.
Une tâche sans règle peut être **comptée** par la règle d'une autre (match kind/metadata) ;
batcher hors-ordre créerait une inversion de priorité (les sans-règles basse priorité
gonfleraient le compteur et bloqueraient une tâche haute priorité avec règle).

### Hors scope de ce lot
Cache conservateur des Capacity keys (un échec de capacité ⇒ skip de la classe pour le
reste de l'itération). À considérer seulement si un backlog massif bloqué par Capacity
rules apparaît en pratique (elles ne sont pas cachables dans `ko`, cf. commentaire dans
`claim_phase`).

### Tests exigés
- **Régression head-of-line** : N tâches bloquées par une règle de concurrence en tête de
  file (priorité haute), 1 tâche éligible derrière (priorité basse) ⇒ elle est claim dans
  la même itération.
- Cap respecté : pas plus de `claim_cap` claims par itération, et l'itération suivante
  reprend le reste.
- Batch-claim : ordre de priorité respecté, pas de batch au travers d'une tâche à règles.
- Les suites existantes `test_priority` et `test_concurrency` restent vertes.
- Mettre à jour la description de `WORKER_START_BATCH_SIZE` dans CLAUDE.md
  (« max claims par itération », plus « max fetched »).

---

## Lot 2 — Outbox transactionnel — ✅ FAIT (2026-06-11)

Validation : 162 tests d'intégration (156 + 6 nouveaux dans `test_outbox.rs`) + 30
unitaires verts, vérifiés indépendamment de l'agent d'implémentation. Décisions prises
à l'implémentation : enqueue inconditionnel (la boucle marque `success` les lignes sans
action) ; `next_attempt_at NOT NULL DEFAULT now()` ; payload enrichi sous une clé
réservée `arcrun` (merge non destructif dans un body custom) ; la boucle de livraison
ne traite que les triggers `end`/`cancel` (les `start` restent synchrones).

Suivi noté en relecture — ✅ FAIT (2026-06-11) : `run_delivery_once` ne tient plus UNE
transaction sur tout le batch. Découpage en 4 phases : (1) **claim court avec lease**
(`claim_due_outbox_leased` : SELECT mûr + `next_attempt_at = now() + WEBHOOK_DELIVERY_LEASE_SECS`
en un `UPDATE … FROM (SELECT … FOR UPDATE SKIP LOCKED) … RETURNING`, puis commit ; le bail
empêche le re-claim concurrent, expire au crash ⇒ at-least-once, et n'incrémente PAS
`attempts`) ; (2) **prefetch hors-lock** (lectures autocommit, états terminaux immuables ;
fast-paths task/batch absent ⇒ success, payload batch malformé ⇒ exhausted, zéro action ⇒
success) ; (3) **livraison HTTP hors-tx en parallèle** via
`futures_util::stream::buffer_unordered(WEBHOOK_DELIVERY_CONCURRENCY)`, aucune connexion
tenue, actions d'une même ligne séquentielles ; (4) **marks en statements courts autocommit**
(`mark_outbox_*` ; une erreur de mark est loggée et ignorée — ne rollback plus les marks déjà
posés ; le bail relivre). Depuis le correctif du 2026-10-08, la signature est
`run_delivery_once(evaluator, pool, cfg)` : la connexion est effectivement rendue
avant HTTP, puis reprise une fois pour les résultats du lot. Le pilotage des tests
reste déterministe. 2 nouveaux env : `WEBHOOK_DELIVERY_LEASE_SECS` (210,
>= 1), `WEBHOOK_DELIVERY_CONCURRENCY` (10, >= 1). 4 nouveaux tests d'intégration dans
`tests/integration/test_delivery_lease.rs` (anti double-claim, expiration du bail,
parallélisme wall-clock, indépendance des marks). 180 tests d'intégration verts.

Transforme `webhook_execution` (déjà : `idempotency_key UNIQUE`, `status`, `attempts`)
en transactional outbox. Résout : pool starvation par webhooks lents dans le chemin HTTP
(`update_running_task` fire les webhooks avant de répondre, connexion tenue jusqu'à 10 s),
webhooks perdus en cas de crash post-commit, absence de retry.

1. **Migration** : valeur `exhausted` dans `webhook_execution_status` ; colonnes
   `next_attempt_at TIMESTAMPTZ`, `last_error TEXT` ; index partiel
   `(next_attempt_at) WHERE status = 'pending'`.
2. **Insertion in-tx** : `update_running_task`, `cancel_task`, `timeout_task_and_propagate`,
   `fail_task_and_propagate`, `stop_batch` + ancêtres dead-end insèrent leurs lignes
   `pending` (tâche + cascade + ancêtres) **dans la transaction** du changement de statut,
   et n'appellent plus reqwest. Exception : `on_start` reste synchrone dans la `start_loop`
   (control-flow, cf. contrat).
3. **Boucle de livraison** (5e worker, à côté de start/timeout/batch/retention) :
   `FOR UPDATE SKIP LOCKED` sur les `pending` mûrs, backoff exponentiel, max N tentatives
   puis `exhausted` + métrique. Ordre par tâche : un `end` attend que le `start` de la même
   tâche ne soit plus `pending`.
4. **Payload enrichi** : statut final + timestamp de transition dans le corps du webhook
   (en plus de `X-Task-Id`/`X-Task-Trigger`/`Idempotency-Key`).
5. **Observabilité** : `GET /webhook-deliveries?status=exhausted`, métriques
   retry/exhausted/lag de livraison.
6. **Tests** : fenêtre de crash (ligne `pending` survit et est livrée au redémarrage),
   retry après échec aval, pas de double livraison, ordre start-avant-end par tâche,
   « PATCH répond vite même si l'aval est lent ».
7. **Documentation du contrat** (section en tête de ce fichier) dans CLAUDE.md/README.

Coût accepté : une écriture par transition (dans une tx déjà ouverte) + latence de
livraison = intervalle de la boucle (configurable ; option : wake-up immédiat via canal).

---

## Lot 3 — ✅ FAIT (2026-06-11)

> Initialement « sur preuve de besoin » ; lancé sur décision utilisateur le 2026-06-11.
>
> **Implémenté** : 3a (insertion groupée multi-row, dedupe-aware) + 3b (webhook
> `on_batch_complete` via l'outbox). 174 tests d'intégration verts (162 + 12 nouveaux
> dans `test_batch_complete.rs`), 30 unitaires. Décisions prises à l'implémentation :
> - UUID tâches générés app-side (`Uuid::new_v4()`) ; `NewTask` porte désormais un champ `id`.
> - Détection batch-complete centralisée dans `maybe_enqueue_batch_complete[_for_task]`
>   (`src/db/webhook_execution.rs`), appelée depuis update_running_task, fail_task_and_propagate,
>   stop_batch, timeout_task_and_propagate, cancel_task, et add_task (batch vide).
> - `webhook_execution.task_id` nullable + `batch_id` nullable + `CHECK (task_id OR batch_id)`.
> - Gate start-avant-end : les lignes `batch_complete` (task_id NULL) ne sont jamais retenues
>   par ce gate (la corrélation `s.task_id = we.task_id` est NULL ⇒ jamais vraie) ; inoffensif
>   car l'état (toutes tâches terminales) est déjà la vérité au moment de l'enqueue.
> - Payload `arcrun` = `{batch_id, counts:{success,failure,canceled}, completed_at}`,
>   counts/`completed_at = max(ended_at)` calculés à la livraison ; pas de `?handle=`.
> - Rétention : cleanup des lignes `batch` orphelines + leurs lignes `webhook_execution` batch-level.

**Relecture indépendante — 2 bugs corrigés** (régressions dans `test_batch_complete.rs`,
validation finale : 176 tests d'intégration + 30 unitaires verts) :
1. **Write-skew sur la détection batch-complete** : deux transactions terminant chacune
   l'une des deux dernières tâches du batch pouvaient, sous READ COMMITTED, chacune voir
   l'autre tâche encore non-terminale ⇒ aucune n'enqueueait, signal perdu. Fix :
   `maybe_enqueue_batch_complete` verrouille la ligne `batch` (`FOR UPDATE`, statement
   séparé) AVANT le check de terminalité — la seconde transaction attend le commit de la
   première et re-vérifie sur un snapshot frais. Le verrou batch est toujours le DERNIER
   pris dans la transaction ⇒ pas de cycle de deadlock.
   Test : `test_batch_complete_concurrent_last_two_tasks_no_lost_signal`.
2. **Orphan-sweep de la rétention vs batch vide** : un batch vide (tout dédupe-skipped)
   n'a aucune tâche ⇒ « orphelin » dès sa création ; le cleanup pouvait supprimer sa
   ligne outbox `pending` avant livraison. Fix : l'orphan-sweep exclut les batches ayant
   une ligne `webhook_execution` encore `pending`.
   Test : `test_cleanup_spares_batch_with_pending_signal`.

Également durci en relecture : le `down.sql` de la migration purge les lignes outbox
batch-level avant de restaurer `task_id NOT NULL` (rollback toujours réalisable), et
`cleanup_old_terminal_tasks` est exposé `pub` pour pilotage déterministe par les tests.

### 3a — Insertion de batch groupée dans `add_task`

Problème : N+1 — `insert_new_task` fait ~4 round-trips par tâche (dedupe check, INSERT
task, INSERT links, 1-3 INSERT actions), en boucle dans la transaction de `add_task`.

Contraintes actées :
- **UUID générés côté application** (`Uuid::new_v4()` dans le handler) : permet de
  construire l'`id_mapping` complet AVANT toute insertion, et donc les links sans
  RETURNING intermédiaire.
- **Batching limité aux runs contigus de tâches sans `dedupe_strategy`** — la dédup est
  évaluée séquentiellement dans la transaction et peut matcher une tâche insérée plus tôt
  **dans le même batch** ; un INSERT multi-valeurs au travers d'une tâche à dédup
  casserait cette sémantique. Même pattern que le garde-fou du batch-claim (Lot 1) :
  on insère le run [tâches sans dédup], on évalue la tâche à dédup, on reprend.
- Dans un run : 1 INSERT multi-valeurs `task`, 1 INSERT multi-valeurs `link`,
  1 INSERT multi-valeurs `action` (start/failure/success de toutes les tâches du run).
- Sémantique préservée à l'identique : parent dédupe-skipped ⇒ dépendance ignorée avec
  warn (comportement existant), compteurs `wait_*`, statut initial Waiting/Pending,
  métriques par tâche.

### 3b — Webhook `on_batch_complete`

Le « signal de fin ordonné » : un webhook batch-level déclenché quand la **dernière**
tâche du batch devient terminale. Notification at-least-once via l'outbox du Lot 2.

Décisions actées :
- **API** : `POST /task` accepte DEUX formes de body (serde untagged, rétro-compatible) :
  le tableau nu existant `[NewTaskDto…]`, ou un objet
  `{"tasks": [NewTaskDto…], "on_batch_complete": [NewActionDto…]}`.
  Validation SSRF des params comme pour les autres actions.
- **Stockage** : nouvelle table `batch (id UUID PK, on_complete JSONB NOT NULL,
  created_at)`. Une ligne créée **uniquement** si `on_batch_complete` est fourni
  (les batches sans webhook ne coûtent rien).
- **Détection** : dans CHAQUE transaction où des tâches deviennent terminales
  (update_running_task, fail_task_and_propagate, timeout, cancel_task, stop_batch,
  y compris cascade + ancêtres dead-end), après propagation : si la tâche a un
  `batch_id` avec ligne `batch`, vérifier
  `NOT EXISTS (task WHERE batch_id = $1 AND status NOT IN (terminaux))`
  ⇒ enqueue outbox in-tx. L'unicité de la clé d'idempotence rend la détection
  concurrente inoffensive (ON CONFLICT DO NOTHING).
- **Outbox** : `webhook_execution.task_id` devient NULLABLE + colonne `batch_id UUID`
  NULLABLE + CHECK (l'un des deux non NULL) ; valeur `batch_complete` ajoutée à
  `trigger_kind` ; condition = sentinel `Success` ; clé = `batch:<batch_id>:complete`.
  La delivery loop traite ces lignes par une branche dédiée : charge `batch.on_complete`,
  exécute les actions SANS `?handle=` (pas de tâche à piloter), payload `arcrun` =
  `{batch_id, counts par statut terminal, completed_at}`.
- **Batch vide** (toutes les tâches dédupe-skipped alors qu'`on_batch_complete` est
  fourni) : le signal est enqueué immédiatement dans la même transaction — un batch de
  zéro tâche est vacuement complet ; le consommateur ne doit pas attendre indéfiniment.
- **Rétention** : le cleanup supprime aussi les lignes `batch` dont plus aucune tâche
  n'existe (et leurs lignes `webhook_execution` batch-level), pour ne pas croître sans
  borne.

### Écarté

- ~~CTE récursive pour `propagate_to_children`~~ — écarté : la logique par niveau
  (échecs `requires_success` + décréments + transition Pending) est délicate en SQL pur et
  la version actuelle est déjà batchée par niveau. Rapport risque/gain défavorable.

## Améliorations notées, non planifiées

- `BATCH_CHANNEL_CAPACITY` (défaut 100) petit pour un endpoint « high-throughput » :
  à augmenter si le PUT devient un goulot.
- Pagination OFFSET des listings : keyset si la pagination profonde devient un usage réel.
- Découplage des appels HTTP et du remplissage des workers : réalisé le 2026-10-08.
  Les claims sont désormais bornés aux places disponibles, sans file de Claimed en
  attente d'un sémaphore. Les réponses outbox sont persistées au fil de l'eau, avec
  priorité aux résultats prêts et conservation bornée lors d'une erreur SQL/pool.
  Les leases, tokens et transactions d'admission/transition restent dans PostgreSQL.
  Voir [Workers](workers.md) et [Webhooks](webhooks.md) pour le comportement actuel.
