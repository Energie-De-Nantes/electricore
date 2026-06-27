# Estimation de provision des lissés — dérivation cœur-pure en kWh, amorcée par R67 (cold-start)

Status: accepted

## Contexte

Un *contrat lissé* facture chaque mois une *provision d'énergie* (kWh), aujourd'hui
*fixée manuellement à la souscription par estimation de la consommation* (cf. glossaire
`core/CONTEXT.md`). La *régularisation* (#191) solde le mesuré contre le provisionné, mais
le retour à l'équilibre doit reposer **d'abord sur l'adaptation des provisions** — « la
régularisation est le solde, pas le thermostat ». Il manquait l'outil qui produit cette
estimation de consommation de façon *data-driven*.

`flux_r67` ([ADR-0047](0047-flux-r67-energie-par-periode-distributeur-hors-releves.md),
mesures facturantes M023, ~36 mois par cadran) fournit l'historique nécessaire pour
**amorcer** (cold-start) la provision d'un nouvel entrant *CFNE* qui n'a pas encore
d'historique mesuré côté EDN. ADR-0047 §10 a explicitement différé cette dérivation comme
« brique séparée de #191, côté core→Odoo » — c'est l'objet de cette ADR. C'est aussi le
point 6 du découpage de l'épique #191 (« Outils provision : estimation initiale +
réévaluation périodique (kWh) ») et l'« Ouverture » de #214.

## Décision

On bâtit une capacité **cœur-pure** d'estimation de provision pour les lissés, qui
transforme un historique d'énergie par cadran en une **estimation de consommation annuelle
par cadran**, puis une provision mensuelle **`/12` plate**. Décisions structurantes :

1. **Sortie = quantités physiques uniquement** : kWh par cadran (annuel + `/12` plate) +
   **métadonnées d'estimation** (couverture temporelle, régime source, qualité).
   **Aucun €** — le prix énergie fournisseur appartient à Odoo
   ([ADR-0016](0016-core-erp-agnostique.md) / ADR-0027, même split que l'accise). Les
   projections de coût régulé (TURPE/CTA/Accise en €), l'optimisation FTA et le conseil
   fournisseur (l'« Ouverture » de #214) sont **hors de cette ADR**.

2. **Abstraction partagée, R67 d'abord** : un *profil cadran*
   `{pdl, cadran → kWh sur fenêtre datée}` alimente un étage commun d'effondrement annuel ;
   seul l'**adaptateur cold-start R67** est livré ici, l'adaptateur *établi* (historique
   R151/R64 propre, réévaluation périodique) se branchera dessus sans refonte. R67
   **contourne** `calculer_periodes_energie` (énergie déjà différenciée, pas index cumulé —
   ADR-0047, le piège « énergie de l'énergie »).

3. **Effondrement = tranche de 12 mois glissants**, somme par cadran ; couverture < 12 mois
   → **flag** dans les métadonnées **+ alerte** en aval (la lib expose un signal alertable,
   elle ne crie pas — patron escalade [ADR-0037](0037-trousseau-cles-aes.md)).

4. **Toutes natures incluses** (R réel + E estimé + C corrigé/régul) = vérité physique
   nette (négatifs préservés, ADR-0047) ; le mix R/E/C devient le **flag qualité** (réelle
   si majorité R, estimée sinon). Pas de clipping.

5. **Sortie WIDE, profondeur cohérente maximale** : `energie_base_kwh` toujours = total ;
   hp/hc et 4-cadrans peuplés **seulement si toute la fenêtre** les porte (sinon null),
   profondeur déclarée en méta ; invariant `base = hp+hc = Σ4`. Odoo collapse vers sa
   *formule tarifaire fournisseur* — le « cadran facturé » est une politique fournisseur,
   hors cœur.

6. **Typage des deux bords** : on définit la **frontière Pandera R67** (« énergie-par-
   période », premier consommateur typé de R67, lève le `validator=None` différé par
   ADR-0047 §9 *pour ce chemin*) et on valide la sortie `RapportProvision`.

7. **Build autonome `RapportProvision`** (à la demande par PDL, pas un champ de
   `ContexteMensuel` — l'amorçage n'a pas de contexte mensuel), livré **bout-en-bout** :
   cœur → endpoint API → commande bot (`/provision <pdl>`).

## Périmètre — les non-s

- Le **déclenchement M023** reste manuel (portail, #216 wontfix) ; la PR suppose le R67
  déjà atterri.
- Le **régime établi / réévaluation** (historique R151/R64 propre) et le **write-back
  Odoo** de la provision sont différés.
- Aucune **valorisation €** ni connaissance des prix fournisseur dans le cœur.
- Cette capacité est **en amont** de #191 : la *régularisation* dérive de provisions bien
  estimées (thermostat), elle ne les calcule pas — l'estimation ne calcule jamais un solde.

## Conséquences

- **MES/PMES** (nouvel occupant, ~⅓ des entrées) n'ont pas de profondeur R67 (coupe à la
  MES) → repli manuel ; la qualité « surtout estimé » des non-Linky remonte dans les
  métadonnées plutôt que de fausser silencieusement la provision.
- Premier livrable de la dérivation de provision ; il **pose le patron** (profil cadran,
  métadonnées couverture/qualité, frontière R67 typée) que l'adaptateur *établi* et #191
  réutiliseront.
