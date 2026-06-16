# Index normalisés en kWh entiers (floor) au boundary dbt

## Contexte

Les index d'Enedis arrivent en **Wh** : R151 et R64 portent `unite='Wh'` (valeurs
~10⁷–10⁸), tandis que les relevés *avant*/*après* de C15 sont nativement en **kWh entiers**
(~10⁴). Les colonnes sont nommées `index_*_kwh` (convention `grandeur_cadran_unité` —
le suffixe *est* l'unité).

Historiquement, la conversion Wh→kWh (avec `floor`) vivait dans les transforms Polars du
loader — `expr_wh_to_kwh` : `when unite='Wh' then floor(valeur/1000)`, câblé dans
`transform_releves`/`transform_r64`, appliqué au *query-time*.

La bascule relevés canoniques ([#248](https://github.com/Energie-De-Nantes/electricore/issues/248),
[ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)) a remplacé `releves()`
par un passe-plat `SELECT * FROM flux_enedis.releves` (+ cast Float64). **La conversion a été
silencieusement perdue**, et le mart `releves` n'union même plus la colonne `unite` qui aurait
pu la rattraper en aval. Résultat : R151/R64 ~1000× trop grands, mélangés à C15 correct →
énergie, TURPE variable, accise, facturation corrompues. La liste des nettoyages dbt d'ADR-0029
(dates, identité, dedup, enrichissement contractuel) **omettait la normalisation d'unité**.

## Décision

1. **La normalisation Wh→kWh descend en dbt, dans les modèles de linéarisation source**
   (`flux_r151`, `flux_r64`) — pas dans le mart `releves`, pas dans le loader. `valeur` (Wh,
   bigint) → `floor(valeur / 1000)` → **kWh entier** ; `unite` normalisée à `'kWh'` dans le
   même modèle. Les colonnes `index_*_kwh` cessent de mentir à *toute* couche : base, mart,
   API `/flux`.

2. **Floor par index, pas floor de l'énergie.** L'index est matérialisé en kWh entier ; la
   résolution sub-kWh est délibérément abandonnée (grain non facturable — l'analyse fine
   relève des *courbes de charge*, pas des différences d'index).

3. **Le convertisseur côté loader est retiré.** `transform_wh_to_kwh` / `expr_wh_to_kwh` /
   `expr_normalize_unit` sont supprimés (sinon double-division des endpoints `r151()`/`r64()`).
   `expr_arrondir_index_kwh` (pipeline énergie) devient un `floor` sur des entiers, donc un
   no-op → retiré avec son site d'appel.

## Raison

- **La convention de nommage est un contrat dur.** `_kwh` doit signifier kWh à toute couche.
  Convertir à la linéarisation l'honore partout d'un coup et garde littéralement vrai le
  principe d'ADR-0029 — « dbt nettoie une donnée fidèle à la source » : fidèle au *sens* de la
  mesure, dans l'unité déclarée, pas à l'entier brut.

- **Floor par index est sûr : l'erreur télescope.** Pour une suite de relevés I₀…Iₙ sur un
  registre cumulé,
  `Σ [floor(Iₖ) − floor(Iₖ₋₁)] = floor(Iₙ) − floor(I₀)`, donc l'écart au vrai cumul vaut
  `frac(I₀) − frac(Iₙ) ∈ (−1, +1)`. L'erreur de facturation **sur toute la vie d'un registre**
  est donc bornée par **< 1 kWh (~< 0,20 €)** — *pas* par période. Indépendante du nombre de
  périodes et du mélange de sources (`floor(I)` ne dépend que de la valeur, pas du flux qui l'a
  rapportée). Les discontinuités (changement de compteur, de structure tarifaire) segmentent le
  registre ; chaque segment ajoute son propre < 1 kWh, mais leurs bornes viennent de C15 (kWh
  entier natif, `frac = 0`) → souvent zéro. Le *floor de l'énergie* serait plus littéral
  (« kWh réellement consommés entre deux lectures ») mais casserait la parité `/releves` de la
  rc7 — sans gain mesurable au regard du grain facturable.

- **Parité préservée.** Le legacy floorait déjà Wh→kWh par index au loader ; le floored-int-kWh
  produit en dbt reproduit cette sortie, donc l'empreinte canonique `/releves` reste stable.

## Conséquences

- **Régénérer les golden snapshots dbt** : les valeurs R151/R64 changent d'échelle (÷1000) —
  changement attendu, à valider comme tel.
- **Amende [ADR-0029](0029-modele-releves-canonique-dbt-assemble-coeur-arbitre.md)** : ajoute la
  normalisation d'unité à la liste des nettoyages portés par dbt. **Supersède** l'approche
  « conversion au loader » (transforms Polars).
- **Garde-fou** : un test dbt assertant `unite = 'kWh'` (et l'absence de magnitude Wh) sur les
  modèles source, pour que la régression ne puisse pas repasser silencieusement.
- **R15 hors périmètre, à vérifier séparément** : `col_simple`/`col_literal('kWh','unite')` côté
  loader force `unite='kWh'` → R15 n'est *jamais* converti ; si R15 est nativement en Wh, bug
  latent. R15 est absent du mart `releves` aujourd'hui → suivi dédié, pas bloquant.
