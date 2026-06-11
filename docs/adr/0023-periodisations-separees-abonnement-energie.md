# Périodisations séparées abonnement / énergie

## Contexte

Les pipelines `abonnements` et `energie` découpent chacun une ligne de temps en périodes,
avec des mécanismes en miroir : l'abonnement porte la période par sa borne de *début*
(attributs contractuels lus à l'événement, `fin = shift(-1)`), l'énergie par sa borne de
*fin* (`energie = index − index.shift(1)`). Vue du code seul, cette symétrie suggère un
périodiseur unique « ruptures → périodes » dont les deux pipelines seraient des
adaptateurs de contenu — la suggestion a été formulée lors de la revue d'architecture
de juin 2026, et le sera à nouveau par toute revue future si la raison du rejet n'est
pas écrite.

## Décision

**Les deux périodisations restent séparées, chacune sur sa propre ligne de temps.**
Le partage entre pipelines se limite aux *formules* transverses (nb_jours, `mois_annee`,
libellés français, validité de période) — pas aux découpages.

## Raison

L'unification suppose « même ligne de temps, contenus différents ». C'est faux au niveau
des données Enedis :

- **Enedis ne fournit pas de relevé d'index pour les événements sans impact comptage.**
  Un MCT qui ne change que la puissance souscrite crée une rupture d'abonnement (part
  fixe) *sans index à cette borne* — la ligne de temps énergie ne peut pas porter cette
  rupture. Les ruptures d'abonnement ne sont donc pas un sous-ensemble alignable des
  bornes d'énergie.
- **Combler exigerait de stocker l'intégralité des relevés quotidiens R64**, pour aller
  chercher l'index à chaque date de MCT. Écarté : l'usage se concentre sur les relevés
  du 1ᵉʳ du mois, le volume ne se justifie pas.
- **Enedis lui-même facture les deux séparément** : l'abonnement à échoir, l'énergie à
  échue. Les deux grains ne coïncident pas non plus côté facturation réseau.

### Alternatives écartées

- **Périodiseur unique `decouper(timeline, over, colonnes_fin)`** — en plus du problème
  de données ci-dessus, le paramétrage des colonnes-de-fin rend l'interface plus large
  que les deux implémentations réunies (module shallow).
- **Récupérer les index aux dates MCT depuis R64** — voir le point volume ci-dessus.

## Conséquences

- Toute proposition de fusion des découpages doit d'abord lever la contrainte de
  données (relevés aux événements hors comptage), pas seulement la symétrie du code.
- Le partage de formules entre les pipelines de périodes est légitime et souhaitable
  (la duplication de `expr_nb_jours` et le formatage de dates en 3 sites — dont un
  `strftime("%d %B %Y")` en facturation qui sort les mois en anglais — relèvent de ce
  partage, pas de l'unification des découpages).
