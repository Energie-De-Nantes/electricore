# ADR-0054 — Chemin de documentation par rôles, en escalier

## Statut

Accepté — PRD [#555](https://github.com/Energie-De-Nantes/electricore/issues/555), tranche
charpente [#556](https://github.com/Energie-De-Nantes/electricore/issues/556).

- **Prolonge** [ADR-0024](0024-trois-registres-de-savoir.md) (trois registres de savoir : code,
  ADRs, glossaires `CONTEXT.md`) — ce chantier les rend **découvrables**, il ne les déplace pas.
- **S'appuie sur** la lecture « EDN est le premier prototype d'une fédération de communs de
  l'électricité » (ADR-0024) : la documentation s'adresse à plusieurs publics d'un commun, pas
  aux seuls contributeurs de code.

## Contexte

La documentation d'ElectriCore avait dérivé en deux masses sans porte d'entrée par public :

- un `README.md` de 448 lignes qui mélange pitch, architecture, exemples de code, déploiement
  VPS, secrets-as-code, notebooks, dev/tests et roadmap dans un seul document linéaire ;
- un `docs/` d'une vingtaine de fichiers techniques, dont 4 seulement raccordés à une nav MkDocs
  plate (`Accueil`, `Guide facturiste`, `Bot Telegram`, `Notebook opérateur`) — les ~15 autres
  (déploiement, configuration, ingestion, conventions de dates…) n'existent que par lien croisé,
  sans jamais apparaître dans le site publié.

Aucun des deux ne répond à « je suis opérateur·ice, où est mon guide ? » ou « je veux déployer
une instance, par où commencer ? » sans déjà savoir où chercher. Le PRD #555 pose 5 rôles de
lecture (vocabulaire des communs) : usager·ère, opérateur·ice, déployeur·euse, contributeur·ice,
mainteneur·e — et un principe d'**escalier** : un sujet transverse (ex. TURPE) existe à plusieurs
étages, chaque étage racontant autre chose (quoi → comment s'en servir → comment c'est fait →
comment le faire évoluer), avec des passerelles montantes en pied de page. Duplication assumée
mais bornée par ces passerelles, plutôt que par une strate unique qui n'appartient à personne.

## Décision

1. **Nav MkDocs à 5 sections par rôle** — `Comprendre` (usager·ère), `Facturer` (opérateur·ice),
   `Déployer` (déployeur·euse), `Contribuer` (contributeur·ice), `Maintenir` (mainteneur·e) — plus
   `Accueil` en tête, hors section. Les 3 pages existantes du site (`changelog-facturiste`,
   `guide-bot-facturiste`, `operateur-notebook`) rejoignent `Facturer` ; `index.md` devient la
   porte d'entrée à 5 sorties plutôt qu'une 4ᵉ page métier.
2. **L'accueil oriente, il n'explique pas** : une phrase par rôle, un lien vers sa section.
3. **Une charte publiée dans `Contribuer`** fixe par écrit les 5 rôles et le principe d'escalier
   (au lieu de les enterrer dans cet ADR), plus la charte éditoriale : point médian partout,
   tutoiement dans les guides et pages d'orientation, tournures impersonnelles dans la référence
   technique.
4. **Une carte des domaines dans `Contribuer`**, miroir de [CONTEXT-MAP.md](../../CONTEXT-MAP.md),
   qui **lie** les glossaires `CONTEXT.md` colocalisés au code sans les dupliquer ni les déplacer —
   la couture entre le site publié et les registres agent, qui restent à leur place.
5. **`Comprendre` / `Déployer` / `Maintenir` ouvrent avec une page d'index courte** (à qui s'adresse
   la section, ce qui y vivra) : leur contenu réel vient de tranches suivantes (#557/#558/#559),
   cette tranche ne pose que la charpente.
6. **Le README maigrit en panneau indicateur** (~60-100 lignes) : pitch, 5 portes vers le site
   publié, install minimale, licence. Sa matière développeur (architecture, exemples de code,
   patterns) migre — sans dupliquer — vers une page `Développer` de `Contribuer`.

### Alternatives écartées

- **Strates dans la page** (un unique document par sujet, sections dépliables du « quoi » au
  « comment ça évolue ») : pas de porte d'entrée par rôle — la page reste un mur que chacun doit
  traverser en entier pour trouver son étage ; impossible de doser le tutoiement (guide) contre
  l'impersonnel (référence) dans le même flux de texte.
- **Diátaxis pur** (tutoriels / guides pratiques / référence / explications) : classe par *nature*
  du contenu, pas par *public* — un tutoriel TURPE et une référence TURPE coexisteraient sans
  jamais répondre « à qui ça s'adresse ». Le fil directeur d'ElectriCore-commun est le rôle dans
  la fédération (ADR-0024), pas le type éditorial.
- **Deux surfaces séparées** (site public de vulgarisation + wiki technique interne) : duplique la
  maintenance et viole le principe « les 3 registres restent découvrables, pas déplacés » — un
  site scindé recrée exactement la fragmentation que l'escalier corrige, avec une frontière en
  plus à tenir synchronisée.
- **Site d'éducation populaire autonome** (ex. Docusaurus dédié à la vulgarisation, hors MkDocs) :
  chantier à part entière, démesuré pour cette PRD, qui ajouterait un **4ᵉ** emplacement de
  documentation au lieu d'en résorber la fragmentation.

## Conséquences

- La validation MkDocs reste volontairement laxiste (`omitted_files: ignore`, liens non stricts) —
  durcir ce garde-fou est le périmètre de [issue #560](https://github.com/Energie-De-Nantes/electricore/issues/560),
  pas de cette tranche.
- Les ~15 docs techniques restantes (`deploiement.md`, `configuration.md`, `ingestion.md`…) restent
  hors nav ; leur affectation à une section est le périmètre de
  [issue #557](https://github.com/Energie-De-Nantes/electricore/issues/557) (passe de lecture).
- Chaque section vide aujourd'hui (`Comprendre`, `Déployer`, `Maintenir`) porte sa propre dette
  documentée par sa page d'index — pas de fausse promesse datée, juste « ce qui y vivra ».
